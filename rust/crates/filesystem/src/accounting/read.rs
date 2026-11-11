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
/// A later proof's credit refusal returns the nonempty proven prefix. If the
/// first fact cannot be proved, the original failure is returned instead.
pub async fn accounting_page<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    authority: &A,
    objects: &O,
    authority_id: AuthorityId,
    cursor: AccountingCursor,
    maximum_records: u32,
    work: &mut crate::AuthorityWork<'_>,
) -> Result<AccountingPage, AccountingError> {
    let maximum_records = maximum_records.min(256);
    if maximum_records == 0
        || (cursor.sequence == Sequence::GENESIS && cursor.digest != Digest::ZERO)
        || (cursor.sequence == Sequence::GENESIS && cursor.settled_at_micros != 0)
    {
        return Err(AccountingError::InvalidBounds);
    }
    if !work.remaining()?.has_finite_allowances() {
        return Err(AccountingError::InvalidBounds);
    }
    let mut scope = work.scope()?;
    let head = authority.head(authority_id, &mut scope).await?;
    accounting_page_at_head(
        (authority, authority_id),
        objects,
        cursor,
        maximum_records,
        head,
        &mut scope,
    )
    .await
}

async fn accounting_page_at_head<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    authority: (&A, AuthorityId),
    objects: &O,
    cursor: AccountingCursor,
    maximum_records: u32,
    head: crate::foundation::Head,
    work: &mut crate::AuthorityWork<'_>,
) -> Result<AccountingPage, AccountingError> {
    let (authority, authority_id) = authority;
    if cursor.sequence > head.sequence
        || (cursor.sequence == head.sequence && cursor.digest != head.digest)
    {
        return Err(AccountingError::InvalidHistory);
    }
    if head.sequence == Sequence::GENESIS {
        return Err(AccountingError::InvalidHistory);
    }
    let mut head_records = replay(
        authority,
        authority_id,
        Sequence::new(head.sequence.get() - 1),
        1,
        work,
    )
    .await?;
    let head_record = head_records
        .value
        .pop()
        .ok_or(AccountingError::InvalidHistory)?;
    if head_record.sequence != head.sequence
        || head_record.digest != head.digest
        || record_digest(authority_id, &head_record, work)? != head.digest
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
        let anchor = replay(
            authority,
            authority_id,
            Sequence::new(cursor.sequence.get() - 1),
            1,
            work,
        )
        .await?;
        let record = anchor
            .value
            .first()
            .ok_or(AccountingError::InvalidHistory)?;
        if record.sequence != cursor.sequence
            || record.digest != cursor.digest
            || record.settled_at_micros != Some(cursor.settled_at_micros)
            || record_digest(authority_id, record, work)? != record.digest
        {
            return Err(AccountingError::InvalidHistory);
        }
        if decode_workspace_deleted(&record.payload, record.payload.len() as u64).is_ok() {
            return Err(AccountingError::InvalidHistory);
        }
    }
    let first = replay(authority, authority_id, Sequence::GENESIS, 1, work).await?;
    let initial = first.value.first().ok_or(AccountingError::InvalidHistory)?;
    let initial_stamp = verify_record(authority_id, AccountingCursor::genesis(), initial, work)?;
    if initial_stamp > watermark_settled_at_micros
        || (cursor.sequence != Sequence::GENESIS && initial_stamp > cursor.settled_at_micros)
    {
        return Err(AccountingError::InvalidHistory);
    }
    let kind = if let Ok(created) =
        decode_volume_created(&initial.payload, initial.payload.len() as u64, &mut |_| {
            Ok::<_, crate::kernel::VolumeCreatedError>(())
        }) {
        if volume_authority_id(created.volume_id) != authority_id {
            return Err(AccountingError::InvalidHistory);
        }
        AuthorityKind::Volume {
            volume_id: created.volume_id,
            config: created.config,
        }
    } else if let Ok(retained) =
        decode_retention_created(&initial.payload, initial.payload.len() as u64)
    {
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
            facts: Retained::allocate(0, &mut work.accounting())?,
            next: cursor,
            watermark_sequence: head.sequence,
            watermark_digest: head.digest,
            watermark_settled_at_micros,
            complete: true,
            work: work.spent(),
        });
    }
    let mut records = replay(
        authority,
        authority_id,
        cursor.sequence,
        maximum_records,
        work,
    )
    .await?;
    if records.value.is_empty() || records.value.len() > maximum_records as usize {
        return Err(AccountingError::InvalidHistory);
    }
    let mut next = cursor;
    let mut facts = Retained::allocate(records.value.len(), &mut work.accounting())?;
    for record in records.value.drain(..) {
        if record.sequence > head.sequence {
            break;
        }
        let admitted = async {
            let stamp = verify_record(authority_id, next, &record, work)?;
            work.accounting().charge_items(1)?;
            let transition = match &kind {
                AuthorityKind::Volume { volume_id, config }
                    if record.sequence == Sequence::new(1) =>
                {
                    let created = decode_volume_created(
                        &record.payload,
                        record.payload.len() as u64,
                        &mut |_| Ok::<_, crate::kernel::VolumeCreatedError>(()),
                    )
                    .map_err(|_| AccountingError::InvalidHistory)?;
                    let bytes = prove_bytes(
                        objects,
                        created.initial_generation_root,
                        (*volume_id, *config),
                        authority_id,
                        &record,
                        work,
                    )
                    .await?;
                    AccountingTransition::Published {
                        volume_id: *volume_id,
                        generation_root: created.initial_generation_root,
                        logical_regular_file_bytes: bytes,
                    }
                }
                AuthorityKind::Volume { volume_id, config } => {
                    if let Ok(published) = decode_published_generation(
                        &record.payload,
                        record.payload.len() as u64,
                        |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()),
                    ) {
                        if published.volume_id != *volume_id {
                            return Err(AccountingError::InvalidHistory);
                        }
                        let bytes = prove_bytes(
                            objects,
                            published.generation_root,
                            (*volume_id, *config),
                            authority_id,
                            &record,
                            work,
                        )
                        .await?;
                        AccountingTransition::Published {
                            volume_id: *volume_id,
                            generation_root: published.generation_root,
                            logical_regular_file_bytes: bytes,
                        }
                    } else {
                        let deleted =
                            decode_workspace_deleted(&record.payload, record.payload.len() as u64)
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
                    let retained =
                        decode_retention_created(&record.payload, record.payload.len() as u64)
                            .map_err(|_| AccountingError::InvalidHistory)?;
                    if retained.volume_id != *volume_id
                        || retained.kind != *kind
                        || retention_authority_id(
                            retained.volume_id,
                            retained.kind,
                            &retained.label,
                        ) != authority_id
                    {
                        return Err(AccountingError::InvalidHistory);
                    }
                    let bytes = prove_bytes(
                        objects,
                        retained.generation_root,
                        (*volume_id, *config),
                        authority_id,
                        &record,
                        work,
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
            work.admit(WorkCounters {
                items_returned: 1,
                ..WorkCounters::UNCHARGED
            })?;
            Ok::<_, AccountingError>((stamp, transition))
        }
        .await;
        let (stamp, transition) = match admitted {
            Ok(admitted) => admitted,
            Err(AccountingError::Work(WorkError::BudgetExceeded { .. }))
                if !facts.value.is_empty() =>
            {
                break;
            }
            Err(error) => return Err(error),
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
        facts.value.push(AccountingFact {
            authority_id,
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
        work.accounting().record_completed(WorkCounters {
            items_returned: 1,
            ..WorkCounters::UNCHARGED
        })?;
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
        work: work.spent(),
    })
}

async fn replay<A: AsyncAuthorityStore>(
    authority: &A,
    authority_id: AuthorityId,
    after: Sequence,
    maximum_records: u32,
    work: &mut crate::AuthorityWork<'_>,
) -> Result<Retained<Vec<DurableCommit>>, AccountingError> {
    let limit = ReplayLimit {
        records: maximum_records,
        payload_bytes: work.remaining()?.source_bytes_read,
    };
    let (records, allocation) = authority.replay(authority_id, after, limit, work).await?;
    let bytes = (records.capacity() as u64)
        .checked_mul(std::mem::size_of::<DurableCommit>() as u64)
        .ok_or(WorkError::Overflow)?;
    #[cfg(not(target_arch = "wasm32"))]
    let allocation = allocation.map_or(
        crate::kernel::allocation::KernelOwnedAllocation::Standalone,
        crate::kernel::allocation::KernelOwnedAllocation::NativeOriginal,
    );
    #[cfg(target_arch = "wasm32")]
    let allocation = {
        let _ = allocation;
        crate::kernel::allocation::KernelOwnedAllocation::Standalone
    };
    let records = Retained::from_owned(
        crate::kernel::Owned::new(records, allocation, bytes),
        &mut work.accounting(),
    )?;
    if records.value.len() > maximum_records as usize {
        return Err(AccountingError::InvalidHistory);
    }
    Ok(records)
}
