//! Bounded, fail-closed projection of durable filesystem authority evidence.
//!
//! The owner must retain the authenticated generation closure for every unread
//! publication or retention. Stream history alone does not preserve those
//! objects after collection. Missing proof is an error, never a zero-byte fact;
//! this projection establishes no archival custody or permission to collect.

use crate::async_storage::{AsyncAuthorityStore, AsyncObjectStore};
use crate::cancellation::CancellationToken;
use crate::foundation::{
    AuthorityId, Digest, DurableCommit, OperationId, Sequence, VolumeId, authority_commit_digest,
};
use crate::kernel::{
    ClosureLimits, DecodeLimits, RetentionKind, decode_published_generation,
    decode_retention_created, decode_volume_created, decode_workspace_deleted,
    prove_generation_closure, retention_authority_id, volume_authority_id,
};
use crate::model::VolumeConfig;
use crate::performance::{WorkBudget, WorkCounters, WorkError};
use crate::storage::{ObjectId, ReplayLimit};
use thiserror::Error;

const MAX_EVENT_BYTES: u64 = 4096;

/// Exact next position in one authority hash chain.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AccountingCursor {
    /// Last consumed dense sequence.
    pub sequence: Sequence,
    /// Digest at that sequence.
    pub digest: Digest,
    /// Last accepted replicated settlement time, or zero at genesis.
    pub settled_at_micros: u64,
}

impl AccountingCursor {
    /// Empty authority position.
    #[must_use]
    pub const fn genesis() -> Self {
        Self {
            sequence: Sequence::GENESIS,
            digest: Digest::ZERO,
            settled_at_micros: 0,
        }
    }
}

/// Semantic transition proven by one durable FS record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AccountingTransition {
    /// A generation became the initial or later published workspace version.
    Published {
        /// Workspace holding this publication.
        volume_id: VolumeId,
        /// Content-addressed generation identity.
        generation_root: ObjectId,
        /// Authenticated logical file size in this generation.
        logical_regular_file_bytes: u128,
    },
    /// A distinct retention authority began keeping a generation alive.
    Retained {
        /// Volume whose generation is retained.
        volume_id: VolumeId,
        /// Workspace whose deletion releases this retention.
        owner_volume_id: VolumeId,
        /// Retention reason.
        kind: RetentionKind,
        /// Content-addressed generation identity.
        generation_root: ObjectId,
        /// Authenticated logical file size in this generation.
        logical_regular_file_bytes: u128,
    },
    /// A workspace ceased retaining its generation history.
    WorkspaceDeleted {
        /// Workspace whose publications and owned retentions are released.
        volume_id: VolumeId,
    },
}

/// Whether a durable fact can represent one customer operation after regional top-level product attribution.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AccountingOperationClass {
    /// One successful durable FS mutation candidate.
    CustomerMutation,
    /// Supporting retention created as part of a workspace fork.
    InternalForkRetention,
}

/// One authenticated authority fact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountingFact {
    /// Dense sequence within the authority.
    pub sequence: Sequence,
    /// Stable retry identity.
    pub operation_id: OperationId,
    /// Authenticated FS commit digest.
    pub commit_digest: Digest,
    /// Replicated settlement time.
    pub settled_at_micros: u64,
    /// Whether this fact can count as one durable FS mutation.
    pub operation_class: AccountingOperationClass,
    /// Canonically decoded state transition.
    pub transition: AccountingTransition,
}

/// One contiguous page ending at a pinned authority head.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AccountingPage {
    /// Proven facts after the supplied cursor.
    pub facts: Vec<AccountingFact>,
    /// Exact cursor after the last fact.
    pub next: AccountingCursor,
    /// Last sequence at the pinned read head.
    pub watermark_sequence: Sequence,
    /// Digest of the pinned read head.
    pub watermark_digest: Digest,
    /// Replicated time of the pinned head.
    pub watermark_settled_at_micros: u64,
    /// True only if the cursor reached the pinned head; no earlier records remain.
    pub complete: bool,
    /// Measured authority and immutable-proof work performed by this read.
    pub work: WorkCounters,
}

/// Fail-closed projection error.
#[derive(Debug, Error)]
pub enum AccountingError {
    /// Authority backend failed.
    #[error("authority read failed: {0}")]
    Authority(String),
    /// Generation closure was absent, corrupt, or outside admitted bounds.
    #[error("generation proof failed: {0}")]
    Proof(String),
    /// Dense chain, cursor, payload, or authority identity is invalid.
    #[error("accounting authority is malformed or has a replay gap")]
    InvalidHistory,
    /// Backend supplied no replicated settlement time.
    #[error("accounting authority has no trusted settlement time")]
    MissingTime,
    /// Caller requested an invalid page.
    #[error("accounting page bounds are invalid")]
    InvalidBounds,
    /// A finite admitted work allowance was exhausted or overflowed.
    #[error(transparent)]
    Work(#[from] WorkError),
}

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
    let head =({ let __authority_args = (authority_id, budget, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.1, __authority_args.2); let __authority_result = authority.head(__authority_args.0, &mut __authority_work).await; __authority_work.receipt(__authority_result) })
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
    let mut measured = AccountingReadWork { budget, work: WorkCounters::default(), live_bytes: 0 };
    let mut head_records =measured.replay(authority, authority_id, Sequence::new(head.sequence.get() - 1), 1, cancellation).await?;
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
        let anchor =measured.replay(authority, authority_id, Sequence::new(cursor.sequence.get() - 1), 1, cancellation).await?;
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
    let first =measured.replay(authority, authority_id, Sequence::GENESIS, 1, cancellation).await?;
    let initial = first.first().ok_or(AccountingError::InvalidHistory)?;
    let initial_stamp = verify_record(authority_id, AccountingCursor::genesis(), initial)?;
    if initial_stamp > watermark_settled_at_micros
        || (cursor.sequence != Sequence::GENESIS && initial_stamp > cursor.settled_at_micros)
    {
        return Err(AccountingError::InvalidHistory);
    }
    let kind = if let Ok(created) = decode_volume_created(&initial.payload, MAX_EVENT_BYTES, &mut |_| Ok::<_, crate::kernel::VolumeCreatedError>(())) {
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
    let records =measured.replay(authority, authority_id, cursor.sequence, maximum_records, cancellation).await?;
    if records.is_empty() || records.len() > maximum_records as usize {
        return Err(AccountingError::InvalidHistory);
    }
    let mut next = cursor;
    measured.live_bytes = measured.last_replay_bytes(&records, records.capacity())?;
    let facts_bytes = u64::try_from(records.len()).map_err(|_| WorkError::Overflow)?
        .checked_mul(u64::try_from(std::mem::size_of::<AccountingFact>()).map_err(|_| WorkError::Overflow)?)
        .ok_or(WorkError::Overflow)?;
    measured.charge(WorkCounters {
        allocation_operations: 1,
        peak_allocation_bytes: facts_bytes,
        ..WorkCounters::default()
    })?;
    let mut facts = Vec::new();
    facts.try_reserve_exact(records.len()).map_err(|_| AccountingError::InvalidBounds)?;
    measured.live_bytes = u64::try_from(facts.capacity()).map_err(|_| WorkError::Overflow)?
        .checked_mul(u64::try_from(std::mem::size_of::<AccountingFact>()).map_err(|_| WorkError::Overflow)?)
        .and_then(|bytes| bytes.checked_add(measured.live_bytes))
        .ok_or(WorkError::Overflow)?;
    measured.work.peak_allocation_bytes = measured.work.peak_allocation_bytes.max(measured.live_bytes);
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
                let created = decode_volume_created(&record.payload, MAX_EVENT_BYTES, &mut |_| Ok::<_, crate::kernel::VolumeCreatedError>(()))
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
                if let Ok(published) = decode_published_generation(&record.payload, MAX_EVENT_BYTES, |_| Ok::<_, crate::kernel::CanonicalDecodeError>(()))
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
        measured.work.items_returned = measured.work.items_returned
            .checked_add(1).ok_or(WorkError::Overflow)?;
        measured.work.verify(budget)?;
        measured.live_bytes = measured.live_bytes.checked_sub(
            u64::try_from(record.payload.len()).map_err(|_| WorkError::Overflow)?,
        ).ok_or(WorkError::Overflow)?;
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

fn verify_record(
    authority_id: AuthorityId,
    cursor: AccountingCursor,
    record: &DurableCommit,
) -> Result<u64, AccountingError> {
    if record.sequence
        != cursor
            .sequence
            .checked_next()
            .map_err(|_| AccountingError::InvalidHistory)?
        || record.previous_digest != cursor.digest
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
    let stamp = record
        .settled_at_micros
        .filter(|stamp| *stamp > 0)
        .ok_or(AccountingError::MissingTime)?;
    if stamp < cursor.settled_at_micros {
        return Err(AccountingError::InvalidHistory);
    }
    Ok(stamp)
}

async fn prove_published<O: AsyncObjectStore>(
    objects: &O,
    root: ObjectId,
    volume_id: VolumeId,
    config: VolumeConfig,
    authority_id: AuthorityId,
    record: &DurableCommit,
    measured: &mut AccountingReadWork,
    cancellation: &CancellationToken,
) -> Result<AccountingTransition, AccountingError> {
    let bytes = prove_bytes(
        objects, root, volume_id, config, authority_id, record, measured, cancellation,
    ).await?;
    Ok(AccountingTransition::Published {
        volume_id,
        generation_root: root,
        logical_regular_file_bytes: bytes,
    })
}

async fn prove_bytes<O: AsyncObjectStore>(
    objects: &O,
    root: ObjectId,
    volume_id: VolumeId,
    config: VolumeConfig,
    authority_id: AuthorityId,
    record: &DurableCommit,
    measured: &mut AccountingReadWork,
    cancellation: &CancellationToken,
) -> Result<u128, AccountingError> {
    // Borrowed canonical fields, decoded manifest/root, exact payload, and
    // canonical re-encoding are bounded together before any archive decode.
    // Eight source lengths cover both representations and the retained source.
    let evidence_bound = config.limits.maximum_generation_bytes
        .min(measured.remaining()?.peak_allocation_bytes / 8);
    let archived = objects.historical_generation_evidence(
        authority_id, record, evidence_bound, config.limits.maximum_objects_per_generation,
        measured.remaining()?, cancellation,
    ).await.map_err(|failure| AccountingError::Proof(failure.error.to_string()))?;
    measured.backend(archived.work)?;
    if let Some(evidence) = archived.value {
        let original = crate::kernel::decode_historical_generation_evidence(
            &evidence, authority_id, record, evidence_bound,
            config.limits.maximum_objects_per_generation,
        ).map_err(|failure| AccountingError::Proof(failure.to_string()))?;
        if original.manifest.generation_root != root
            || original.root.volume_id != volume_id
            || original.manifest.config != config
        {
            return Err(AccountingError::InvalidHistory);
        }
        return Ok(original.logical_file_bytes);
    }
    let limits = ClosureLimits {
        decode: DecodeLimits {
            maximum_object_bytes: config.limits.maximum_object_bytes,
            maximum_name_bytes: config.limits.maximum_component_bytes,
            maximum_page_items: config.limits.maximum_directory_page_entries,
            maximum_page_bytes: u32::try_from(config.limits.maximum_object_bytes)
                .unwrap_or(u32::MAX),
            maximum_page_height: config.limits.maximum_page_height,
            maximum_visited_pages: u32::try_from(config.limits.maximum_objects_per_generation)
                .unwrap_or(u32::MAX),
        },
        maximum_objects: config.limits.maximum_objects_per_generation,
        maximum_files: config.limits.maximum_files_per_generation,
        maximum_object_bytes: config.limits.maximum_generation_bytes,
        profile: config.profile,
        symbolic_links: config.symbolic_links,
        hard_links: config.hard_links,
        sparse_files: config.sparse_files,
    };
    // Without a provider archive, authenticate only the root named by this
    // historical record. Missing collected bodies fail closed; a current head
    // is never a substitute for the original generation.
    let proof = prove_generation_closure(
        objects, root, limits, measured.remaining()?, cancellation,
    ).await.map_err(|failure| AccountingError::Proof(failure.error.to_string()))?;
    measured.backend(proof.work)?;
    if proof.root.volume_id != volume_id {
        return Err(AccountingError::InvalidHistory);
    }
    Ok(proof.logical_file_bytes)
}

struct AccountingReadWork {
    budget: WorkBudget,
    work: WorkCounters,
    live_bytes: u64,
}

impl AccountingReadWork {
    fn remaining(&self) -> Result<WorkBudget, AccountingError> {
        let mut remaining = self.work.remaining(self.budget)?;
        remaining.peak_allocation_bytes = remaining.peak_allocation_bytes
            .checked_sub(self.live_bytes).ok_or(AccountingError::InvalidBounds)?;
        Ok(remaining)
    }

    fn backend(&mut self, work: WorkCounters) -> Result<(), AccountingError> {
        self.work = self.work.with_backend(work, self.live_bytes)?;
        self.work.verify(self.budget)?;
        Ok(())
    }

    fn charge(&mut self, mut work: WorkCounters) -> Result<(), AccountingError> {
        work.peak_allocation_bytes = work.peak_allocation_bytes
            .checked_add(self.live_bytes).ok_or(WorkError::Overflow)?;
        self.work = self.work.checked_add(work)?;
        self.work.verify(self.budget)?;
        Ok(())
    }

    fn last_replay_bytes(&self, records: &[DurableCommit], capacity: usize) -> Result<u64, WorkError> {
        let payload_bytes = records.iter().try_fold(0_u64, |bytes, record| {
            bytes.checked_add(u64::try_from(record.payload.len()).map_err(|_| WorkError::Overflow)?)
                .ok_or(WorkError::Overflow)
        })?;
        payload_bytes.checked_add(
            u64::try_from(capacity).map_err(|_| WorkError::Overflow)?
                .checked_mul(u64::try_from(std::mem::size_of::<DurableCommit>()).map_err(|_| WorkError::Overflow)?)
                .ok_or(WorkError::Overflow)?,
        ).ok_or(WorkError::Overflow)
    }

    async fn replay<A: AsyncAuthorityStore>(
        &mut self,
        authority: &A,
        authority_id: AuthorityId,
        after: Sequence,
        maximum_records: u32,
        cancellation: &CancellationToken,
    ) -> Result<Vec<DurableCommit>, AccountingError> {
        let receipt =({ let __authority_args = (authority_id, after, ReplayLimit {
                records: maximum_records,
                payload_bytes: MAX_EVENT_BYTES * u64::from(maximum_records),
            }, self.remaining()?, cancellation,); let mut __authority_work = crate::AuthorityWork::new(__authority_args.3, __authority_args.4); let __authority_result = authority.replay(__authority_args.0, __authority_args.1, __authority_args.2, &mut __authority_work).await.map(|(value, _allocation)| value); __authority_work.receipt(__authority_result) }).map_err(|failure| AccountingError::Authority(failure.error.to_string()))?;
        self.backend(receipt.work)?;
        if receipt.value.len() > maximum_records as usize
            || receipt.value.iter().any(|record| record.payload.len() as u64 > MAX_EVENT_BYTES)
        {
            return Err(AccountingError::InvalidHistory);
        }
        Ok(receipt.value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::foundation::Epoch;
    use bytes::Bytes;

    fn commit(
        authority: AuthorityId,
        sequence: u64,
        previous: Digest,
        stamp: Option<u64>,
    ) -> DurableCommit {
        let sequence = Sequence::new(sequence);
        let epoch = Epoch::GENESIS;
        let operation_id = OperationId::from_bytes([3; 16]);
        let fingerprint = Digest::from_bytes([4; 32]);
        let payload = Bytes::from_static(b"fact");
        DurableCommit {
            epoch,
            sequence,
            operation_id,
            fingerprint,
            previous_digest: previous,
            digest: authority_commit_digest(
                authority,
                epoch,
                sequence,
                operation_id,
                fingerprint,
                previous,
                &payload,
            ),
            payload,
            settled_at_micros: stamp,
        }
    }

    #[test]
    fn page_cursor_rejects_gaps_tampering_and_time_regression() {
        let authority = AuthorityId::from_bytes([1; 16]);
        let first = commit(authority, 1, Digest::ZERO, Some(100));
        assert_eq!(
            verify_record(authority, AccountingCursor::genesis(), &first).ok(),
            Some(100)
        );
        let cursor = AccountingCursor {
            sequence: first.sequence,
            digest: first.digest,
            settled_at_micros: 100,
        };
        let second = commit(authority, 2, first.digest, Some(101));
        assert_eq!(verify_record(authority, cursor, &second).ok(), Some(101));
        assert!(matches!(
            verify_record(authority, cursor, &first),
            Err(AccountingError::InvalidHistory)
        ));
        assert!(matches!(
            verify_record(
                authority,
                cursor,
                &commit(authority, 3, first.digest, Some(101))
            ),
            Err(AccountingError::InvalidHistory)
        ));
        assert!(matches!(
            verify_record(
                authority,
                cursor,
                &commit(authority, 2, first.digest, Some(99))
            ),
            Err(AccountingError::InvalidHistory)
        ));
        assert!(matches!(
            verify_record(authority, cursor, &commit(authority, 2, first.digest, None)),
            Err(AccountingError::MissingTime)
        ));
        let mut tampered = second;
        tampered.payload = Bytes::from_static(b"other");
        assert!(matches!(
            verify_record(authority, cursor, &tampered),
            Err(AccountingError::InvalidHistory)
        ));
    }
}
