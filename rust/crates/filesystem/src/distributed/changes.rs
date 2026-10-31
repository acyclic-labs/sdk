//! Change records share the authority CAS; no post-publication relay is involved.
use super::*;
use crate::{PublicationChanges, VolumeConfig};
use acyclic_stream::{AppendRecord, ChangeEntryRef, RecordHeaders};

pub(super) fn changes_path(
    placement: &AuthorityPlacement,
    volume: crate::VolumeId,
    work: &mut AuthorityWork<'_>,
) -> Result<StreamPath, AuthorityStoreError> {
    let mut text = [0; crate::publication_changes::CHANGES_PATH_LEN];
    let text =
        crate::publication_changes::changes_path_text(placement.actor_scope(), volume, &mut text)
            .ok_or(crate::WorkError::Overflow)?;
    work.own_path(text)
}

fn metadata(
    change: &crate::workspace::ChangedPath,
    config: VolumeConfig,
    work: &mut AuthorityWork<'_>,
) -> Result<Vec<u8>, AuthorityStoreError> {
    let length = crate::publication_changes::entry::encoded_len(change, config);
    let writes = codec_work(length as u64, length as u64, 0);
    work.admit(writes)?;
    let mut output = work.vec(length)?;
    crate::publication_changes::entry::encode(change, config, &mut output);
    work.charge(writes)?;
    Ok(output)
}

mod encoding;

struct PreparedEntry {
    key: Bytes,
    operation: &'static str,
    metadata: Vec<u8>,
}
impl PreparedEntry {
    fn borrowed(&self) -> ChangeEntryRef<'_> {
        ChangeEntryRef {
            key: &self.key,
            operation: self.operation,
            metadata: &self.metadata,
        }
    }
}

fn chunk_end(context: &[u8], entries: &[PreparedEntry]) -> Result<usize, AuthorityStoreError> {
    let mut references: [ChangeEntryRef<'_>; acyclic_stream::MAX_RECORD_KEYS] =
        std::array::from_fn(|_| ChangeEntryRef {
            key: &[],
            operation: "",
            metadata: &[],
        });
    let count = entries.len().min(acyclic_stream::MAX_RECORD_KEYS);
    for (slot, entry) in references.iter_mut().zip(entries) {
        *slot = entry.borrowed();
    }
    Ok(acyclic_stream::changes_chunk_len(
        context,
        references.get(..count).ok_or(crate::WorkError::Overflow)?,
    )?)
}

fn records(
    seal: &PublicationChanges,
    sequence: Sequence,
    work: &mut AuthorityWork<'_>,
) -> Result<Vec<AppendRecord>, AuthorityStoreError> {
    let context = crate::PublicationContext {
        volume: seal.volume(),
        generation: GenerationId::from_root(seal.root()),
        authority_sequence: sequence,
        records_after: 0,
    };
    let mut entries = work.vec(seal.paths().len().max(1))?;
    for change in seal.paths() {
        work.check()?;
        let key = crate::publication_changes::entry::key(&change.path, seal.config());
        entries.push(PreparedEntry {
            key: work.bytes(key.slice())?,
            operation: crate::publication_changes::entry::operation(change),
            metadata: metadata(change, seal.config(), work)?,
        });
    }
    if entries.is_empty() {
        entries.push(PreparedEntry {
            key: Bytes::new(),
            operation: "publish",
            metadata: Vec::new(),
        });
    }
    // Native encoding/folding can change byte order. Equal bounded keys cannot
    // share one typed record, so their complete path facts use adjacent chunks.
    entries.sort_unstable_by(|a, b| a.key.cmp(&b.key).then(a.metadata.cmp(&b.metadata)));
    encode_chunks(context, &entries, work)
}

fn encode_chunks(
    context: crate::PublicationContext,
    entries: &[PreparedEntry],
    work: &mut AuthorityWork<'_>,
) -> Result<Vec<AppendRecord>, AuthorityStoreError> {
    // The fixed-width remaining count keeps the original two-pass partition
    // exact: setting each member's count cannot change its encoded length.
    let mut encoded_context = context.encode();
    let mut remaining = entries;
    let mut chunks = 0;
    while !remaining.is_empty() {
        remaining = remaining
            .get(chunk_end(encoded_context.as_ref(), remaining)?..)
            .ok_or(crate::WorkError::Overflow)?;
        chunks += 1;
    }
    let mut output = work.vec(chunks)?;
    let mut remaining = entries;
    while !remaining.is_empty() {
        let count = chunk_end(encoded_context.as_ref(), remaining)?;
        let (selected, next) = remaining.split_at(count);
        let mut references: [ChangeEntryRef<'_>; acyclic_stream::MAX_RECORD_KEYS] =
            std::array::from_fn(|_| ChangeEntryRef {
                key: &[],
                operation: "",
                metadata: &[],
            });
        for (slot, entry) in references.iter_mut().zip(selected) {
            *slot = entry.borrowed();
        }
        encoded_context.set_records_after(
            u64::try_from(chunks - output.len() - 1).map_err(|_| crate::WorkError::Overflow)?,
        );
        output.push(encoding::entries(
            encoded_context.as_ref(),
            references.get(..count).ok_or(crate::WorkError::Overflow)?,
            work,
        )?);
        remaining = next;
    }
    Ok(output)
}

impl<P: crate::distributed::AuthorityProvider + ?Sized> StreamAuthorityStore<P> {
    pub(super) fn publication_changes(
        &self,
        authority: AuthorityId,
        proposed: &ProposedCommit,
        generation: GenerationId,
        base: Option<Head>,
        sequence: Sequence,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(StreamPath, Vec<AppendRecord>), AuthorityStoreError> {
        let seal = proposed
            .publication
            .as_ref()
            .ok_or(AuthorityStoreError::RejectedStatic(
                "publication has no authenticated namespace changes",
            ))?;
        if volume_authority_id(seal.volume()) != authority
            || seal.root() != generation.object_id()
            || seal.base() != base
        {
            return Err(AuthorityStoreError::RejectedStatic(
                "namespace changes belong to another publication",
            ));
        }
        Ok((
            changes_path(&self.placement, seal.volume(), work)?,
            records(seal, sequence, work)?,
        ))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "changes_tests.rs"]
mod tests;
