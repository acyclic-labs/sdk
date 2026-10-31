//! Source and epoch checkpoints contribute to the original conversation cut.
use super::*;
use crate::context::{
    SkillPublicationChange, SkillPublicationSource, SkillSourceCheckpoint, SourceCursor,
};
use crate::conversation::epochs::Snapshot;
use acyclic_stream::CommitCondition;

/// Original identity and optimistic journal position for one context publication.
pub struct ContextPublicationRequest<'a> {
    /// Original operation, including retries after acknowledgement loss.
    pub operation: OperationId,
    /// Original publication key within that operation.
    pub key: &'a str,
    /// Original execution step.
    pub step: u32,
    /// Exclusive operation-journal position, not a raw conversation cursor.
    pub expected_tail: u64,
    /// Resolved profile already pinned by the actor.
    pub profile: [u8; 32],
}

/// Journal-owned destinations and the last admitted original source cursor.
/// The paths retain their original funding and cannot be supplied by a caller.
pub struct ContextCheckpoint<'io> {
    epoch: checkpoint::IndexPath<'io>,
    source: checkpoint::IndexPath<'io>,
    owner: [u8; 32],
    volume: [u8; 16],
    selected: [u8; 32],
    cursor: SourceCursor,
    tail: Option<u64>,
}

impl ContextCheckpoint<'_> {
    /// Original durable cursor used when opening the one authorized source follow.
    pub fn cursor(&self) -> SourceCursor {
        self.cursor
    }
}

/// Complete conditional receipt authority for this exact pending source.
/// Keep it while installing the already-prepared catalog/registry state.
/// Dropping it leaves the original source pending for replay; it does not
/// fabricate confirmation or withdraw an already committed publication.
pub struct ContextPublicationReceipt<'a, 'io> {
    source: &'a mut SkillPublicationSource,
    checkpoint: &'a mut ContextCheckpoint<'io>,
    cursor: SourceCursor,
    tail: u64,
}

impl ContextPublicationReceipt<'_, '_> {
    /// Borrows the original pinned metadata; no body is reopened or copied.
    pub fn changes(&self) -> Result<&[SkillPublicationChange]> {
        Ok(self
            .source
            .pending()?
            .ok_or_else(|| invalid(None))?
            .changes())
    }

    /// Confirms only this receipt's source after the caller installs its
    /// prepared state. No caller-selected cursor, path or closure flag exists.
    pub fn confirm(self) -> Result<()> {
        self.source.confirm_publication(self.cursor)?;
        self.checkpoint.cursor = self.cursor;
        self.checkpoint.tail = Some(self.tail);
        Ok(())
    }
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: crate::store::JournalProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Binds one source to this agent-private journal. Cold recovery reads only
    /// its last checkpoint; subsequent admissions use the retained fence.
    /// The caller must retain the original admitted session, never a turn's
    /// deadline, for the lifetime of the returned paths and source subscription.
    pub fn bind_context_source<'io>(
        &'io self,
        volume_ref: &VolumeRef,
        volume_id: acyclic_fs::VolumeId,
        initial: SourceCursor,
        io: &'io PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'io, ContextCheckpoint<'io>>> {
        let owner = self.index_volume_digest(Some(io))?;
        let selected = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::json::canonical_json_digest(
                volume_ref,
                io.remaining()?.bytes_encoded,
                work,
            )
        })?;
        let volume = volume_id.into_bytes();
        let mut cursor = initial;
        let epoch_name = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(
                format_args!(
                    "harness/v1/context/{}/epoch",
                    blake3::Hash::from_bytes(owner)
                ),
                work,
            )
        })?;
        let source_name = crate::executor::with_history_io(Some(io), |work| {
            crate::executor::preparation_format(
                format_args!(
                    "harness/v1/context/{}/source/{}",
                    blake3::Hash::from_bytes(owner),
                    blake3::Hash::from_bytes(selected)
                ),
                work,
            )
        })?;
        let epoch = checkpoint::IndexPath::from_name(epoch_name, Some(io))?;
        let path = checkpoint::IndexPath::from_name(source_name, Some(io))?;
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            crate::executor::with_history_io(Some(io), |work| {
                ContentGrant::authorize_volume(
                    &self.verifier,
                    &self.scope,
                    &self.volume,
                    VolumeOperation::Read,
                    super::MAXIMUM_PAYLOAD_BYTES,
                    work,
                )
            })?;
            let mut history = io.history_io();
            let mut work = Some(&mut history as &mut dyn crate::store::HistoryIo);
            let tail =
                crate::store::operations::tail_optional_with_work(&self.stream, &path, &mut work)
                    .await?;
            if let Some(sequence) = tail.and_then(|tail| tail.checked_sub(1)) {
                let record =
                    crate::store::operations::one_record(&self.stream, &path, sequence, &mut work)
                        .await?
                        .ok_or_else(|| invalid(Some(io)))?;
                let record_hold = io.guard_owned(
                    crate::store::operations::sdk_record_retained_bytes(&record, &work),
                )?;
                let (stored, hold) = SkillSourceCheckpoint::decode(&record.value, io)?;
                if stored.volume() != volume {
                    return Err(invalid(Some(io)));
                }
                cursor = stored.cursor();
                drop(stored);
                drop(hold);
                drop(record);
                drop(record_hold);
            } else if tail.is_some() {
                return Err(invalid(Some(io)));
            }
            Ok(ContextCheckpoint {
                epoch,
                source: path,
                owner,
                volume,
                selected,
                cursor,
                tail,
            })
        })
    }

    /// Publishes the producer checkpoint, exact M2 snapshot and selected context
    /// changes through the existing conditional journal/index request. This is
    /// not raw exchange closure, a new submit path or a session allocator.
    pub fn publish_context<'a, 'io>(
        &'a self,
        checkpoint: &'a mut ContextCheckpoint<'io>,
        source: &'a mut SkillPublicationSource,
        request: ContextPublicationRequest<'a>,
        epoch: &'a Snapshot,
        io: &'a PreparationIo,
    ) -> Result<
        crate::conversation::BoundedContentFuture<'a, Option<ContextPublicationReceipt<'a, 'io>>>,
    > {
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            self.require_operation(request.operation, Some(io))?;
            if self.execution.is_none()
                || self.index_volume_digest(Some(io))? != checkpoint.owner
                || epoch.epoch().profile != request.profile
            {
                return Err(invalid(Some(io)));
            }
            let selected = crate::executor::with_history_io(Some(io), |work| {
                crate::executor::json::canonical_json_digest(
                    source.volume(),
                    io.remaining()?.bytes_encoded,
                    work,
                )
            })?;
            if selected != checkpoint.selected || source.executing_volume() != checkpoint.volume {
                return Err(invalid(Some(io)));
            }
            let cut = source.pending()?.ok_or_else(|| invalid(Some(io)))?;
            if cut.from() != checkpoint.cursor || cut.executing_volume() != checkpoint.volume {
                return Err(invalid(Some(io)));
            }
            for change in cut.changes() {
                let pinned = match change {
                    SkillPublicationChange::Skill(metadata) => {
                        crate::executor::with_history_io(Some(io), |work| metadata.validate(work))?;
                        &metadata.source
                    }
                    SkillPublicationChange::Removed(pinned) => pinned,
                };
                self.authorize_context_source(pinned, Some(io))?;
            }
            let cursor = cut.cursor();
            let next_tail = checkpoint
                .tail
                .unwrap_or(0)
                .checked_add(1)
                .ok_or_else(|| invalid(Some(io)))?;
            let source_record = cut.encode_checkpoint(io)?;
            let source_hold = io.guard_bytes(&source_record)?;
            let identity = crate::model::hash_bytes(&source_record, Some(io))?;
            let encoded_epoch = epoch.encode((*checkpoint.epoch).clone(), io)?;
            let (epoch_condition, epoch_record) = encoded_epoch.parts();
            let source_condition = match checkpoint.tail {
                None => CommitCondition::Absent {
                    path: (*checkpoint.source).clone(),
                },
                Some(expected) => CommitCondition::Tail {
                    path: (*checkpoint.source).clone(),
                    expected,
                },
            };
            // Fixed owner-derived order: epoch precedes source in this namespace.
            let contributions = [
                indexed_publication::Contribution {
                    condition: epoch_condition.clone(),
                    record: epoch_record,
                },
                indexed_publication::Contribution {
                    condition: source_condition,
                    record: &source_record,
                },
            ];
            let bytes = crate::conversation::envelope::encode_context_batch(
                request.operation,
                request.profile,
                identity,
                cut.changes(),
                io,
            )?;
            let bytes_hold = io.guard_bytes(&bytes)?;
            // The original source revalidates after preparation and immediately
            // before the only publication; a conditional loser retains its cut.
            source.pending()?.ok_or_else(|| invalid(Some(io)))?;
            let committed = self
                .append_conversation_bytes(
                    request.operation,
                    request.expected_tail,
                    request.key,
                    request.step,
                    &bytes,
                    &contributions,
                    io,
                )
                .await?;
            drop(bytes);
            drop(bytes_hold);
            drop(contributions);
            drop(encoded_epoch);
            drop(source_record);
            drop(source_hold);
            Ok(committed.then_some(ContextPublicationReceipt {
                source,
                checkpoint,
                cursor,
                tail: next_tail,
            }))
        })
    }
}

fn invalid(io: Option<&PreparationIo>) -> Error {
    canonical_boundary(
        io,
        crate::wire::ErrorCode::Conflict,
        "context publication differs from its original source or epoch checkpoint",
    )
}
