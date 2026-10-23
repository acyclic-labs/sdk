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

fn encode_entry(
    context: &[u8],
    entry: ChangeEntryRef<'_>,
    work: &mut AuthorityWork<'_>,
) -> Result<AppendRecord, AuthorityStoreError> {
    #[cfg(not(target_arch = "wasm32"))]
    if work.is_native() {
        return AppendRecord::changes_native(context, &[entry], &work.native_meter()?)
            .map_err(|error| work.native_io_failure(error))?
            .map_err(Into::into);
    }
    let mut keys = work.vec(1)?;
    let key = work.bytes(entry.key)?;
    keys.push(key.clone().into());
    let mut entries = work.vec(1)?;
    // The String owns an admitted buffer rather than an unaccounted copy.
    let mut operation_text = work.vec(entry.operation.len())?;
    operation_text.extend_from_slice(entry.operation.as_bytes());
    entries.push(acyclic_stream::wire::ChangeEntry {
        key,
        operation: String::from_utf8(operation_text).map_err(|_| crate::WorkError::Overflow)?,
        metadata: work.bytes(entry.metadata)?,
    });
    let change = acyclic_stream::wire::ChangeRecord {
        context: work.bytes(context)?,
        entries,
    };
    let encoded_len = change.encoded_len();
    if encoded_len + entry.key.len() + b"acyclic.recordchanges/v1".len()
        > acyclic_stream::MAX_RECORD_BYTES
    {
        return Err(acyclic_stream::StreamError::LimitExceeded.into());
    }
    let mut value = work.vec(encoded_len)?;
    let writes = codec_work(encoded_len as u64, encoded_len as u64, 0);
    work.admit(writes)?;
    change
        .encode(&mut value)
        .map_err(|_| crate::WorkError::Overflow)?;
    work.charge(writes)?;
    let mut attributes = work.vec(1)?;
    let mut name = work.vec(b"acyclic.record".len())?;
    name.extend_from_slice(b"acyclic.record");
    attributes.push(Attribute {
        name: String::from_utf8(name).map_err(|_| crate::WorkError::Overflow)?,
        value: b"changes/v1".as_slice().into(),
    });
    work.accounting()
        .claim_owned(RecordHeaders::control_bytes() as u64, 1)
        .map_err(authority_error)?;
    let headers = RecordHeaders::new(keys, attributes)?;
    Ok(AppendRecord {
        value: work.plain_bytes(value)?.into(),
        headers,
    })
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
    let mut output = work.vec(seal.paths().len().max(1))?;
    for change in seal.paths() {
        work.check()?;
        let key = path_key(&change.path, seal.config());
        let metadata = metadata(change, work)?;
        let operation = match (change.before.is_some(), change.after.is_some()) {
            (false, true) => "create",
            (true, false) => "remove",
            _ => "update",
        };
        output.push(encode_entry(
            &context,
            ChangeEntryRef {
                key: key.slice(),
                operation,
                metadata: &metadata,
            },
            work,
        )?);
    }
    if output.is_empty() {
        output.push(encode_entry(
            &context,
            ChangeEntryRef {
                key: &[],
                operation: "publish",
                metadata: &[],
            },
            work,
        )?);
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
