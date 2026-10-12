use super::*;

enum AuthorityKind {
    Volume {
        volume_id: VolumeId,
        config: VolumeConfig,
    },
    Retention {
        volume_id: VolumeId,
        kind: RetentionKind,
        config: VolumeConfig,
    },
}

/// Reads and proves at most `maximum_records` records after `cursor`.
#[allow(clippy::too_many_lines)] // The pinned read, anchor checks, and page verification form one fail-closed operation.
pub async fn accounting_page<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    authority: &A,
    objects: &O,
    authority_id: AuthorityId,
    cursor: AccountingCursor,
    maximum_records: u32,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<AccountingPage, AccountingError> {
    if maximum_records == 0
        || maximum_records > 256
        || (cursor.sequence == Sequence::GENESIS && cursor.digest != Digest::ZERO)
        || (cursor.sequence == Sequence::GENESIS && cursor.settled_at_micros != 0)
    {
        return Err(AccountingError::InvalidBounds);
    }
    if !budget.has_finite_allowances() {
        return Err(AccountingError::InvalidBounds);
    }
    let head = ({
        let __authority_args = (authority_id, budget, cancellation);
        let mut __authority_work =
            crate::AuthorityWork::new(__authority_args.1, __authority_args.2);
        let __authority_result = authority
            .head(__authority_args.0, &mut __authority_work)
            .await;
        __authority_work.receipt(__authority_result)
    })
    .map_err(|e| AccountingError::Authority(e.error.to_string()))?;
    accounting_page_at_head(
        authority,
        objects,
        authority_id,
        cursor,
        maximum_records,
        head.value,
        head.work.remaining(budget)?,
        cancellation,
    )
    .await
    .and_then(|mut page| {
        page.work = head.work.checked_add(page.work)?;
        page.work.verify(budget)?;
        Ok(page)
    })
}

pub(crate) async fn accounting_page_at_head<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    authority: &A,
    objects: &O,
    authority_id: AuthorityId,
    cursor: AccountingCursor,
    maximum_records: u32,
    head: crate::foundation::Head,
    budget: WorkBudget,
    cancellation: &CancellationToken,
) -> Result<AccountingPage, AccountingError> {
    if maximum_records == 0
        || maximum_records > 256
        || !budget.has_finite_allowances()
        || (cursor.sequence == Sequence::GENESIS
            && (cursor.digest != Digest::ZERO || cursor.settled_at_micros != 0))
    {
        return Err(AccountingError::InvalidBounds);
    }
    if cursor.sequence > head.sequence
        || (cursor.sequence == head.sequence && cursor.digest != head.digest)
    {
        return Err(AccountingError::InvalidHistory);
    }
    if head.sequence == Sequence::GENESIS {
        return Err(AccountingError::InvalidHistory);
    }
    let mut measured = AccountingReadWork {
        budget,
        work: WorkCounters::default(),
        live_bytes: 0,
    };
    let mut head_records = measured
        .replay(
            authority,
            authority_id,
            Sequence::new(head.sequence.get() - 1),
            1,
            cancellation,
        )
        .await?;
    let head_record = head_records.pop().ok_or(AccountingError::InvalidHistory)?;
    if head_record.sequence != head.sequence
        || head_record.digest != head.digest
        || authority_commit_digest(
            authority_id,
            head_record.epoch,
            head_record.sequence,
            head_record.operation_id,
            head_record.fingerprint,
            head_record.previous_digest,
            &head_record.payload,
        ) != head.digest
    {
        return Err(AccountingError::InvalidHistory);
    }
    let watermark_settled_at_micros = head_record
        .settled_at_micros
        .filter(|stamp| *stamp > 0)
        .ok_or(AccountingError::MissingTime)?;
    if cursor.settled_at_micros > watermark_settled_at_micros
        || (cursor.sequence == head.sequence
            && cursor.settled_at_micros != watermark_settled_at_micros)
    {
        return Err(AccountingError::InvalidHistory);
    }
    drop(head_record);
    drop(head_records);
    if cursor.sequence != Sequence::GENESIS && cursor.sequence < head.sequence {
        let anchor = measured
            .replay(
                authority,
                authority_id,
                Sequence::new(cursor.sequence.get() - 1),
                1,
                cancellation,
            )
            .await?;
        let record = anchor.first().ok_or(AccountingError::InvalidHistory)?;
        if record.sequence != cursor.sequence
            || record.digest != cursor.digest
            || record.settled_at_micros != Some(cursor.settled_at_micros)
            || authority_commit_digest(
                authority_id,
                record.epoch,
                record.sequence,
                record.operation_id,
                record.fingerprint,
                record.previous_digest,
                &record.payload,
            ) != record.digest
        {
            return Err(AccountingError::InvalidHistory);
        }
        if decode_workspace_deleted(&record.payload, MAX_EVENT_BYTES).is_ok() {
            return Err(AccountingError::InvalidHistory);
        }
    }
    let first = measured
        .replay(authority, authority_id, Sequence::GENESIS, 1, cancellation)
        .await?;
    let initial = first.first().ok_or(AccountingError::InvalidHistory)?;
    let initial_stamp = verify_record(authority_id, AccountingCursor::genesis(), initial)?;
    if initial_stamp > watermark_settled_at_micros
        || (cursor.sequence != Sequence::GENESIS && initial_stamp > cursor.settled_at_micros)
    {
        return Err(AccountingError::InvalidHistory);
    }
    let kind = if let Ok(created) =
        decode_volume_created(&initial.payload, MAX_EVENT_BYTES, &mut |_| {
            Ok::<_, crate::kernel::VolumeCreatedError>(())
        }) {
        if volume_authority_id(created.volume_id) != authority_id {
            return Err(AccountingError::InvalidHistory);
        }
        AuthorityKind::Volume {
            volume_id: created.volume_id,
            config: created.config,
        }
    } else if let Ok(retained) = decode_retention_created(&initial.payload, MAX_EVENT_BYTES) {
        if retention_authority_id(retained.volume_id, retained.kind, &retained.label)
            != authority_id
        {
            return Err(AccountingError::InvalidHistory);
        }
        AuthorityKind::Retention {
            volume_id: retained.volume_id,
            kind: retained.kind,
            config: retained.config,
        }
    } else {
        return Err(AccountingError::InvalidHistory);
    };
    drop(first);
    if cursor.sequence == head.sequence {
        return Ok(AccountingPage {
            facts: Vec::new(),
            next: cursor,
            watermark_sequence: head.sequence,
            watermark_digest: head.digest,
            watermark_settled_at_micros,
            complete: true,
            work: measured.work,
        });
    }
    let records = measured
        .replay(
            authority,
            authority_id,
            cursor.sequence,
            maximum_records,
            cancellation,
        )
        .await?;
    if records.is_empty() || records.len() > maximum_records as usize {
        return Err(AccountingError::InvalidHistory);
    }
    let mut next = cursor;
    measured.live_bytes = measured.last_replay_bytes(&records, records.capacity())?;
    let facts_bytes = u64::try_from(records.len())
        .map_err(|_| WorkError::Overflow)?
        .checked_mul(
            u64::try_from(std::mem::size_of::<AccountingFact>())
                .map_err(|_| WorkError::Overflow)?,
        )
        .ok_or(WorkError::Overflow)?;
    measured.charge(WorkCounters {
        allocation_operations: 1,
        peak_allocation_bytes: facts_bytes,
        ..WorkCounters::default()
    })?;
    let mut facts = Vec::new();
    facts
        .try_reserve_exact(records.len())
        .map_err(|_| AccountingError::InvalidBounds)?;
    measured.live_bytes = u64::try_from(facts.capacity())
        .map_err(|_| WorkError::Overflow)?
        .checked_mul(
            u64::try_from(std::mem::size_of::<AccountingFact>())
                .map_err(|_| WorkError::Overflow)?,
        )
        .and_then(|bytes| bytes.checked_add(measured.live_bytes))
        .ok_or(WorkError::Overflow)?;
    measured.work.peak_allocation_bytes =
        measured.work.peak_allocation_bytes.max(measured.live_bytes);
    measured.work.verify(budget)?;
    for record in records.into_iter().take(maximum_records as usize) {
        if record.sequence > head.sequence {
            break;
        }
        let stamp = verify_record(authority_id, next, &record)?;
        measured.charge(WorkCounters {
            items_examined: 1,
            bytes_hashed: crate::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES
                .checked_add(u64::try_from(record.payload.len()).map_err(|_| WorkError::Overflow)?)
                .ok_or(WorkError::Overflow)?,
            ..WorkCounters::default()
        })?;
        let transition = match &kind {
            AuthorityKind::Volume { volume_id, config } if record.sequence == Sequence::new(1) => {
                let created = decode_volume_created(&record.payload, MAX_EVENT_BYTES, &mut |_| {
                    Ok::<_, crate::kernel::VolumeCreatedError>(())
                })
                .map_err(|_| AccountingError::InvalidHistory)?;
                prove_published(
                    objects,
                    created.initial_generation_root,
                    *volume_id,
                    *config,
                    authority_id,
                    &record,
                    &mut measured,
                    cancellation,
                )
                .await?
            }
            AuthorityKind::Volume { volume_id, config } => {
                if let Ok(published) =
                    decode_published_generation(&record.payload, MAX_EVENT_BYTES, |_| {
                        Ok::<_, crate::kernel::CanonicalDecodeError>(())
                    })
                {
                    if published.volume_id != *volume_id {
                        return Err(AccountingError::InvalidHistory);
                    }
                    prove_published(
                        objects,
                        published.generation_root,
                        *volume_id,
                        *config,
                        authority_id,
                        &record,
                        &mut measured,
                        cancellation,
                    )
                    .await?
                } else {
                    let deleted = decode_workspace_deleted(&record.payload, MAX_EVENT_BYTES)
                        .map_err(|_| AccountingError::InvalidHistory)?;
                    if deleted != *volume_id || record.sequence != head.sequence {
                        return Err(AccountingError::InvalidHistory);
                    }
                    AccountingTransition::WorkspaceDeleted { volume_id: deleted }
                }
            }
            AuthorityKind::Retention {
                volume_id,
                kind,
                config,
            } => {
                if record.sequence != Sequence::new(1) {
                    return Err(AccountingError::InvalidHistory);
                }
                let retained = decode_retention_created(&record.payload, MAX_EVENT_BYTES)
                    .map_err(|_| AccountingError::InvalidHistory)?;
                if retained.volume_id != *volume_id
                    || retained.kind != *kind
                    || retention_authority_id(retained.volume_id, retained.kind, &retained.label)
                        != authority_id
                {
                    return Err(AccountingError::InvalidHistory);
                }
                let bytes = prove_bytes(
                    objects,
                    retained.generation_root,
                    *volume_id,
                    *config,
                    authority_id,
                    &record,
                    &mut measured,
                    cancellation,
                )
                .await?;
                let owner_volume_id = match retained.kind {
                    RetentionKind::ForkBase => hex::decode(&retained.label)
                        .ok()
                        .and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
                        .map(VolumeId::from_bytes)
                        .ok_or(AccountingError::InvalidHistory)?,
                    RetentionKind::Checkpoint | RetentionKind::Pin => *volume_id,
                };
                AccountingTransition::Retained {
                    volume_id: *volume_id,
                    owner_volume_id,
                    kind: *kind,
                    generation_root: retained.generation_root,
                    logical_regular_file_bytes: bytes,
                }
            }
        };
        let operation_class = match &transition {
            AccountingTransition::Retained {
                kind: RetentionKind::ForkBase,
                ..
            } => AccountingOperationClass::InternalForkRetention,
            AccountingTransition::Published { .. }
            | AccountingTransition::Retained { .. }
            | AccountingTransition::WorkspaceDeleted { .. } => {
                AccountingOperationClass::CustomerMutation
            }
        };
        facts.push(AccountingFact {
            sequence: record.sequence,
            operation_id: record.operation_id,
            commit_digest: record.digest,
            settled_at_micros: stamp,
            operation_class,
            transition,
        });
        next = AccountingCursor {
            sequence: record.sequence,
            digest: record.digest,
            settled_at_micros: stamp,
        };
        measured.work.items_returned = measured
            .work
            .items_returned
            .checked_add(1)
            .ok_or(WorkError::Overflow)?;
        measured.work.verify(budget)?;
        measured.live_bytes = measured
            .live_bytes
            .checked_sub(u64::try_from(record.payload.len()).map_err(|_| WorkError::Overflow)?)
            .ok_or(WorkError::Overflow)?;
    }
    let complete = next.sequence == head.sequence;
    if complete
        && (next.digest != head.digest || next.settled_at_micros != watermark_settled_at_micros)
    {
        return Err(AccountingError::InvalidHistory);
    }
    Ok(AccountingPage {
        facts,
        next,
        watermark_sequence: head.sequence,
        watermark_digest: head.digest,
        watermark_settled_at_micros,
        complete,
        work: measured.work,
    })
}

pub(super) struct AccountingReadWork {
    budget: WorkBudget,
    work: WorkCounters,
    live_bytes: u64,
}

impl AccountingReadWork {
    pub(super) fn remaining(&self) -> Result<WorkBudget, AccountingError> {
        let mut remaining = self.work.remaining(self.budget)?;
        remaining.peak_allocation_bytes = remaining
            .peak_allocation_bytes
            .checked_sub(self.live_bytes)
            .ok_or(AccountingError::InvalidBounds)?;
        Ok(remaining)
    }

    pub(super) fn backend(&mut self, work: WorkCounters) -> Result<(), AccountingError> {
        self.work = self.work.with_backend(work, self.live_bytes)?;
        self.work.verify(self.budget)?;
        Ok(())
    }

    fn charge(&mut self, mut work: WorkCounters) -> Result<(), AccountingError> {
        work.peak_allocation_bytes = work
            .peak_allocation_bytes
            .checked_add(self.live_bytes)
            .ok_or(WorkError::Overflow)?;
        self.work = self.work.checked_add(work)?;
        self.work.verify(self.budget)?;
        Ok(())
    }

    fn last_replay_bytes(
        &self,
        records: &[DurableCommit],
        capacity: usize,
    ) -> Result<u64, WorkError> {
        let payload_bytes = records.iter().try_fold(0_u64, |bytes, record| {
            bytes
                .checked_add(u64::try_from(record.payload.len()).map_err(|_| WorkError::Overflow)?)
                .ok_or(WorkError::Overflow)
        })?;
        payload_bytes
            .checked_add(
                u64::try_from(capacity)
                    .map_err(|_| WorkError::Overflow)?
                    .checked_mul(
                        u64::try_from(std::mem::size_of::<DurableCommit>())
                            .map_err(|_| WorkError::Overflow)?,
                    )
                    .ok_or(WorkError::Overflow)?,
            )
            .ok_or(WorkError::Overflow)
    }

    async fn replay<A: AsyncAuthorityStore>(
        &mut self,
        authority: &A,
        authority_id: AuthorityId,
        after: Sequence,
        maximum_records: u32,
        cancellation: &CancellationToken,
    ) -> Result<Vec<DurableCommit>, AccountingError> {
        let receipt = ({
            let __authority_args = (
                authority_id,
                after,
                ReplayLimit {
                    records: maximum_records,
                    payload_bytes: MAX_EVENT_BYTES * u64::from(maximum_records),
                },
                self.remaining()?,
                cancellation,
            );
            let mut __authority_work =
                crate::AuthorityWork::new(__authority_args.3, __authority_args.4);
            let __authority_result = authority
                .replay(
                    __authority_args.0,
                    __authority_args.1,
                    __authority_args.2,
                    &mut __authority_work,
                )
                .await
                .map(|(value, _allocation)| value);
            __authority_work.receipt(__authority_result)
        })
        .map_err(|failure| AccountingError::Authority(failure.error.to_string()))?;
        self.backend(receipt.work)?;
        if receipt.value.len() > maximum_records as usize
            || receipt
                .value
                .iter()
                .any(|record| record.payload.len() as u64 > MAX_EVENT_BYTES)
        {
            return Err(AccountingError::InvalidHistory);
        }
        Ok(receipt.value)
    }
}
