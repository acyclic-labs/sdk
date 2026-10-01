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
    prove_generation_closure_async, retention_authority_id, volume_authority_id,
};
use crate::model::VolumeConfig;
use crate::performance::WorkBudget;
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

/// Whether a durable fact can represent one customer operation after regional
/// top-level product attribution.
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

/// Reads and proves at most `maximum_records` records after `cursor`. The
/// authority head is pinned before replay; a later append belongs to the next
/// page. Every record must be dense, hash-chained, canonically decoded and
/// stamped by the replicated Stream authority.
#[allow(clippy::too_many_lines)] // The pinned read, anchor checks, and page verification form one fail-closed operation.
pub async fn accounting_page<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    authority: &A,
    objects: &O,
    authority_id: AuthorityId,
    cursor: AccountingCursor,
    maximum_records: u32,
    cancellation: &CancellationToken,
) -> Result<AccountingPage, AccountingError> {
    if maximum_records == 0
        || maximum_records > 256
        || (cursor.sequence == Sequence::GENESIS && cursor.digest != Digest::ZERO)
        || (cursor.sequence == Sequence::GENESIS && cursor.settled_at_micros != 0)
    {
        return Err(AccountingError::InvalidBounds);
    }
    let head = authority
        .head(authority_id, WorkBudget::UNBOUNDED, cancellation)
        .await
        .map_err(|e| AccountingError::Authority(e.error.to_string()))?
        .value;
    accounting_page_at_head(
        authority,
        objects,
        authority_id,
        cursor,
        maximum_records,
        head,
        cancellation,
    )
    .await
}

pub(crate) async fn accounting_page_at_head<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    authority: &A,
    objects: &O,
    authority_id: AuthorityId,
    cursor: AccountingCursor,
    maximum_records: u32,
    head: crate::foundation::Head,
    cancellation: &CancellationToken,
) -> Result<AccountingPage, AccountingError> {
    if maximum_records == 0
        || maximum_records > 256
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
    let head_record = authority
        .replay(
            authority_id,
            Sequence::new(head.sequence.get() - 1),
            ReplayLimit {
                records: 1,
                payload_bytes: MAX_EVENT_BYTES,
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(|e| AccountingError::Authority(e.error.to_string()))?
        .value
        .into_iter()
        .next()
        .ok_or(AccountingError::InvalidHistory)?;
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
    if cursor.sequence != Sequence::GENESIS && cursor.sequence < head.sequence {
        let anchor = authority
            .replay(
                authority_id,
                Sequence::new(cursor.sequence.get() - 1),
                ReplayLimit {
                    records: 1,
                    payload_bytes: MAX_EVENT_BYTES,
                },
                WorkBudget::UNBOUNDED,
                cancellation,
            )
            .await
            .map_err(|e| AccountingError::Authority(e.error.to_string()))?
            .value;
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
    }
    let first = authority
        .replay(
            authority_id,
            Sequence::GENESIS,
            ReplayLimit {
                records: 1,
                payload_bytes: MAX_EVENT_BYTES,
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(|e| AccountingError::Authority(e.error.to_string()))?
        .value;
    let initial = first.first().ok_or(AccountingError::InvalidHistory)?;
    if initial.sequence != Sequence::new(1) {
        return Err(AccountingError::InvalidHistory);
    }
    let kind = if let Ok(created) = decode_volume_created(&initial.payload, MAX_EVENT_BYTES) {
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
    if cursor.sequence == head.sequence {
        return Ok(AccountingPage {
            facts: Vec::new(),
            next: cursor,
            watermark_sequence: head.sequence,
            watermark_digest: head.digest,
            watermark_settled_at_micros,
            complete: true,
        });
    }
    let records = authority
        .replay(
            authority_id,
            cursor.sequence,
            ReplayLimit {
                records: maximum_records,
                payload_bytes: MAX_EVENT_BYTES * u64::from(maximum_records),
            },
            WorkBudget::UNBOUNDED,
            cancellation,
        )
        .await
        .map_err(|e| AccountingError::Authority(e.error.to_string()))?
        .value;
    if records.is_empty() || records.len() > maximum_records as usize {
        return Err(AccountingError::InvalidHistory);
    }
    let mut next = cursor;
    let mut facts = Vec::with_capacity(records.len());
    for record in records.into_iter().take(maximum_records as usize) {
        if record.sequence > head.sequence {
            break;
        }
        let stamp = verify_record(authority_id, next, &record)?;
        let transition = match &kind {
            AuthorityKind::Volume { volume_id, config } if record.sequence == Sequence::new(1) => {
                let created = decode_volume_created(&record.payload, MAX_EVENT_BYTES)
                    .map_err(|_| AccountingError::InvalidHistory)?;
                prove_published(
                    objects,
                    created.initial_generation_root,
                    *volume_id,
                    *config,
                    cancellation,
                )
                .await?
            }
            AuthorityKind::Volume { volume_id, config } => {
                if let Ok(published) = decode_published_generation(&record.payload, MAX_EVENT_BYTES)
                {
                    if published.volume_id != *volume_id {
                        return Err(AccountingError::InvalidHistory);
                    }
                    prove_published(
                        objects,
                        published.generation_root,
                        *volume_id,
                        *config,
                        cancellation,
                    )
                    .await?
                } else {
                    let deleted = decode_workspace_deleted(&record.payload, MAX_EVENT_BYTES)
                        .map_err(|_| AccountingError::InvalidHistory)?;
                    if deleted != *volume_id {
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
    cancellation: &CancellationToken,
) -> Result<AccountingTransition, AccountingError> {
    let bytes = prove_bytes(objects, root, volume_id, config, cancellation).await?;
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
    cancellation: &CancellationToken,
) -> Result<u128, AccountingError> {
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
    let proof =
        prove_generation_closure_async(objects, root, limits, WorkBudget::UNBOUNDED, cancellation)
            .await
            .map_err(|e| AccountingError::Proof(e.error.to_string()))?;
    if proof.root.volume_id != volume_id {
        return Err(AccountingError::InvalidHistory);
    }
    Ok(proof.logical_file_bytes)
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
