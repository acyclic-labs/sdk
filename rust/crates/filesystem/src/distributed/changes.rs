//! Change records share the authority CAS; no post-publication relay is involved.
use super::*;
use crate::kernel::{LogicalName, NameEncoding, NamespacePath};
use crate::model::CaseSensitivity;
use crate::{PublicationChanges, VolumeConfig};
use acyclic_stream::{AppendRecord, Attribute, ChangeEntryRef, RecordHeaders};
use prost::Message;
use std::fmt::Write;

/// Root-relative Unicode keys match access-policy paths. Non-Unicode native
/// components use an impossible UTF-8 prefix followed by their exact encoding.
/// Only the first 1 KiB is indexed. A bounded [start,end) containing this prefix
/// contains all its extensions: projection cannot widen any representable range.
struct PathKey {
    bytes: [u8; acyclic_stream::MAX_RECORD_KEY_BYTES],
    len: usize,
}
impl PathKey {
    fn push(&mut self, bytes: &[u8]) {
        let count = bytes.len().min(self.bytes.len() - self.len);
        if let Some(destination) = self.bytes.get_mut(self.len..self.len + count) {
            destination.copy_from_slice(bytes.get(..count).unwrap_or_default());
            self.len += count;
        }
    }
    fn slice(&self) -> &[u8] {
        self.bytes.get(..self.len).unwrap_or_default()
    }
}
impl Write for PathKey {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.push(text.as_bytes());
        Ok(())
    }
}

fn path_key(path: &NamespacePath, config: VolumeConfig) -> PathKey {
    let mut key = PathKey {
        bytes: [0; acyclic_stream::MAX_RECORD_KEY_BYTES],
        len: 0,
    };
    for (index, name) in path.components().iter().enumerate() {
        if index != 0 {
            key.push(b"/");
        }
        component_key(name, config, &mut key);
    }
    key
}

fn component_key(name: &LogicalName, config: VolumeConfig, key: &mut PathKey) {
    match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            if let Ok(text) = std::str::from_utf8(name.as_bytes()) {
                if config.case_sensitivity == CaseSensitivity::ProfileFolded {
                    let _ = NameEncoding::native_for_profile(config.profile)
                        .write_unicode_case_fold_key(text, key);
                } else {
                    key.push(text.as_bytes());
                }
                return;
            }
        }
        NameEncoding::WindowsUtf16Le => {
            let units = || {
                name.as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|b| u16::from_le_bytes(*b))
            };
            if char::decode_utf16(units()).all(|c| c.is_ok()) {
                for value in char::decode_utf16(units()).flatten() {
                    let mut bytes = [0; 4];
                    key.push(value.encode_utf8(&mut bytes).as_bytes());
                }
                return;
            }
        }
    }
    key.push(&[0xff, name.encoding().tag()]);
    for byte in name.as_bytes() {
        let mut text = [0; 2];
        if hex::encode_to_slice([*byte], &mut text).is_ok() {
            key.push(&text);
        }
    }
}

pub(super) fn changes_path(
    volume: crate::VolumeId,
    work: &mut AuthorityWork<'_>,
) -> Result<StreamPath, AuthorityStoreError> {
    let mut text = [0; 44];
    text.get_mut(..12)
        .ok_or(crate::WorkError::Overflow)?
        .copy_from_slice(b".acyclic/fs/");
    hex::encode_to_slice(
        volume.into_bytes(),
        text.get_mut(12..).ok_or(crate::WorkError::Overflow)?,
    )
    .map_err(|_| crate::WorkError::Overflow)?;
    let text = std::str::from_utf8(&text).map_err(|_| crate::WorkError::Overflow)?;
    work.own_path(text)
}

fn metadata(
    change: &crate::workspace::ChangedPath,
    work: &mut AuthorityWork<'_>,
) -> Result<Vec<u8>, AuthorityStoreError> {
    let length = change
        .path
        .components()
        .iter()
        .try_fold(4usize, |n, name| n.checked_add(5 + name.as_bytes().len()))
        .and_then(|n| n.checked_add(2 * (1 + 16 + 1 + 32)))
        .ok_or(crate::WorkError::Overflow)?;
    let writes = codec_work(length as u64, length as u64, 0);
    work.admit(writes)?;
    let mut output = work.vec(length)?;
    output.extend_from_slice(
        &u32::try_from(change.path.depth())
            .map_err(|_| crate::WorkError::Overflow)?
            .to_le_bytes(),
    );
    for name in change.path.components() {
        output.push(name.encoding().tag());
        output.extend_from_slice(
            &u32::try_from(name.as_bytes().len())
                .map_err(|_| crate::WorkError::Overflow)?
                .to_le_bytes(),
        );
        output.extend_from_slice(name.as_bytes());
    }
    for record in [change.before, change.after] {
        output.push(u8::from(record.is_some()));
        if let Some(record) = record {
            output.extend_from_slice(&record.file_id.into_bytes());
            output.push(record.kind.tag());
            output.extend_from_slice(record.metadata.digest.as_bytes());
        }
    }
    work.charge(codec_work(output.len() as u64, output.len() as u64, 0))?;
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

    fn record_bytes(&self) -> usize {
        fn field_len(length: usize) -> usize {
            if length == 0 {
                0
            } else {
                1 + prost::encoding::encoded_len_varint(length as u64) + length
            }
        }
        let body = field_len(self.key.len())
            + field_len(self.operation.len())
            + field_len(self.metadata.len());
        // Entry field plus its indexed header key; the fixed context and marker
        // are accounted for once by chunk_end.
        1 + prost::encoding::encoded_len_varint(body as u64) + body + self.key.len()
    }
}

fn chunk_end(entries: &[PreparedEntry]) -> Result<usize, AuthorityStoreError> {
    let mut bytes = 1 + 1 + 56 + b"acyclic.recordchanges/v1".len();
    let mut previous: Option<&[u8]> = None;
    let mut count = 0;
    for entry in entries.iter().take(acyclic_stream::MAX_RECORD_KEYS) {
        let next = bytes
            .checked_add(entry.record_bytes())
            .ok_or(crate::WorkError::Overflow)?;
        if next > acyclic_stream::MAX_RECORD_BYTES
            || previous.is_some_and(|key| key == entry.key.as_ref())
        {
            break;
        }
        bytes = next;
        previous = Some(&entry.key);
        count += 1;
    }
    if count == 0 {
        return Err(acyclic_stream::StreamError::LimitExceeded.into());
    }
    Ok(count)
}

fn records(
    seal: &PublicationChanges,
    sequence: Sequence,
    work: &mut AuthorityWork<'_>,
) -> Result<Vec<AppendRecord>, AuthorityStoreError> {
    let mut context = [0; 56];
    context
        .get_mut(..16)
        .ok_or(crate::WorkError::Overflow)?
        .copy_from_slice(&seal.volume().into_bytes());
    context
        .get_mut(16..48)
        .ok_or(crate::WorkError::Overflow)?
        .copy_from_slice(seal.root().digest.as_bytes());
    context
        .get_mut(48..)
        .ok_or(crate::WorkError::Overflow)?
        .copy_from_slice(&sequence.get().to_le_bytes());
    let mut entries = work.vec(seal.paths().len().max(1))?;
    for change in seal.paths() {
        work.check()?;
        let key = path_key(&change.path, seal.config());
        entries.push(PreparedEntry {
            key: work.bytes(key.slice())?,
            operation: match (change.before.is_some(), change.after.is_some()) {
                (false, true) => "create",
                (true, false) => "remove",
                _ => "update",
            },
            metadata: metadata(change, work)?,
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
    encode_chunks(&context, &entries, work)
}

fn encode_chunks(
    context: &[u8],
    entries: &[PreparedEntry],
    work: &mut AuthorityWork<'_>,
) -> Result<Vec<AppendRecord>, AuthorityStoreError> {
    let mut remaining = entries;
    let mut chunks = 0;
    while !remaining.is_empty() {
        remaining = remaining
            .get(chunk_end(remaining)?..)
            .ok_or(crate::WorkError::Overflow)?;
        chunks += 1;
    }
    let mut output = work.vec(chunks)?;
    let mut remaining = entries;
    while !remaining.is_empty() {
        let count = chunk_end(remaining)?;
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
        output.push(encoding::entries(
            context,
            references.get(..count).ok_or(crate::WorkError::Overflow)?,
            work,
        )?);
        remaining = next;
    }
    Ok(output)
}

impl<P: acyclic_stream::StreamProvider> StreamAuthorityStore<P> {
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
            || seal.root().kind != crate::ObjectKind::GenerationRoot
            || seal.root().digest != generation.digest()
            || seal.base() != base
        {
            return Err(AuthorityStoreError::RejectedStatic(
                "namespace changes belong to another publication",
            ));
        }
        Ok((
            changes_path(seal.volume(), work)?,
            records(seal, sequence, work)?,
        ))
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "changes_tests.rs"]
mod tests;
