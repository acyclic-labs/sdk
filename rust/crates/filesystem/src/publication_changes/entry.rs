//! The one codec for a filesystem change entry's path-local facts: the
//! producer that writes them into a `changes/v2` record and every reader.
//!
//! Metadata is, with every integer a minimal LEB128 varint:
//! `names records`. `names` is `0 tag` when the entry's key is exactly its
//! path's names joined by `/` in one encoding `tag`, so the key alone is the
//! lossless path; otherwise it is `depth+1 (tag len name)*`. `records` holds
//! the before and after file records the operation implies, each
//! `file_id(16) kind(1) metadata_digest(32)`.
use crate::VolumeConfig;
use crate::kernel::{FileRecord, LogicalName, NameEncoding, NamespacePath};
use crate::model::CaseSensitivity;
use crate::workspace::ChangedPath;
use std::fmt::Write;

/// Root-relative Unicode keys match access-policy paths. Non-Unicode native
/// components use an impossible UTF-8 prefix followed by their exact encoding.
/// Only the first 1 KiB is indexed. A bounded [start,end) containing this prefix
/// contains all its extensions: projection cannot widen any representable range.
pub(crate) struct PathKey {
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
    pub(crate) fn slice(&self) -> &[u8] {
        self.bytes.get(..self.len).unwrap_or_default()
    }
}
impl Write for PathKey {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        self.push(text.as_bytes());
        Ok(())
    }
}

/// The indexed key of `path`.
pub(crate) fn key(path: &NamespacePath, config: VolumeConfig) -> PathKey {
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

const RECORD_BYTES: usize = 16 + 1 + 32;

/// The semantic operation of a change: its before and after presence.
pub(crate) fn operation(change: &ChangedPath) -> &'static str {
    match (change.before.is_some(), change.after.is_some()) {
        (false, true) => "create",
        (true, false) => "remove",
        _ => "update",
    }
}

fn varint_len(value: usize) -> usize {
    prost::encoding::encoded_len_varint(value as u64)
}

/// A name's exact Unicode text, when it has one: its key component then is
/// that text, verbatim, in a volume that does not fold case.
fn text_len(name: &LogicalName) -> Option<usize> {
    match name.encoding() {
        NameEncoding::Utf8 | NameEncoding::PosixBytes => {
            std::str::from_utf8(name.as_bytes()).ok().map(str::len)
        }
        NameEncoding::WindowsUtf16Le => {
            let units = name
                .as_bytes()
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| u16::from_le_bytes(*b));
            char::decode_utf16(units)
                .try_fold(0, |total, unit| unit.ok().map(|c| total + c.len_utf8()))
        }
    }
}

/// The one encoding of a path whose key is exactly its names: Unicode names
/// in one encoding, in a volume that does not fold case, short enough that
/// the key is not truncated. The key then is the lossless path.
fn names_from_key(path: &NamespacePath, config: VolumeConfig) -> Option<NameEncoding> {
    let first = path.components().first()?.encoding();
    if config.case_sensitivity == CaseSensitivity::ProfileFolded {
        return None;
    }
    let mut joined = path.components().len() - 1;
    for name in path.components() {
        if name.encoding() != first || name.as_bytes().is_empty() {
            return None;
        }
        joined += text_len(name)?;
    }
    (joined < acyclic_stream::MAX_RECORD_KEY_BYTES).then_some(first)
}

/// Exact encoded length of `change`'s metadata.
pub(crate) fn encoded_len(change: &ChangedPath, config: VolumeConfig) -> usize {
    let names = if names_from_key(&change.path, config).is_some() {
        2
    } else {
        varint_len(change.path.depth() + 1)
            + change
                .path
                .components()
                .iter()
                .map(|name| 1 + varint_len(name.as_bytes().len()) + name.as_bytes().len())
                .sum::<usize>()
    };
    names
        + RECORD_BYTES
            * (usize::from(change.before.is_some()) + usize::from(change.after.is_some()))
}

/// Writes `change`'s metadata, `encoded_len` bytes, after `output`'s contents.
pub(crate) fn encode(change: &ChangedPath, config: VolumeConfig, output: &mut Vec<u8>) {
    if let Some(encoding) = names_from_key(&change.path, config) {
        output.extend_from_slice(&[0, encoding.tag()]);
    } else {
        prost::encoding::encode_varint(change.path.depth() as u64 + 1, output);
        for name in change.path.components() {
            output.push(name.encoding().tag());
            prost::encoding::encode_varint(name.as_bytes().len() as u64, output);
            output.extend_from_slice(name.as_bytes());
        }
    }
    for record in [change.before.as_ref(), change.after.as_ref()]
        .into_iter()
        .flatten()
    {
        write_record(record, output);
    }
}

fn write_record(record: &FileRecord, output: &mut Vec<u8>) {
    output.extend_from_slice(&record.file_id.into_bytes());
    output.push(record.kind.tag());
    output.extend_from_slice(record.metadata.digest.as_bytes());
}

/// Lossless path and presence facts decoded by the filesystem publication codec.
///
/// These facts do not grant access. Obtain entries through the authorized change
/// provider and retain the original record's origin, sequence and commit identity.
#[derive(Debug, PartialEq, Eq)]
pub struct PublicationEntry {
    /// Complete native path; the bounded index key alone may be truncated or folded.
    pub path: NamespacePath,
    /// Whether a file record existed at this path before the publication.
    pub before_present: bool,
    /// Whether a file record exists at this path after the publication.
    pub after_present: bool,
}

impl PublicationEntry {
    /// Decodes one entry with the same codec used by authorized change follows.
    ///
    /// `create`, `remove` and `update` imply absent/present, present/absent and
    /// present/present respectively. `publish` has no path-local records and is
    /// valid only with an empty key and metadata. Use the volume's original config.
    ///
    /// # Errors
    /// Returns `InvalidArgument` for an unknown operation, invalid native path,
    /// malformed file records, noncanonical framing or trailing bytes.
    pub fn from_parts(
        key: &[u8],
        operation: &str,
        metadata: &[u8],
        config: VolumeConfig,
    ) -> Result<Self, acyclic_stream::StreamError> {
        read::entry(key, operation, metadata, config)
            .map_err(|_| acyclic_stream::StreamError::InvalidArgument)
    }
}

/// The reader shared by the public projection, authorized follow and controls.
mod read {
    use super::*;
    use crate::kernel::FileKind;

    /// A malformed entry; readers map it to their own error.
    #[derive(Debug, PartialEq, Eq)]
    pub(crate) struct Malformed;

    fn presence(operation: &str) -> Option<[bool; 2]> {
        match operation {
            "create" => Some([false, true]),
            "remove" => Some([true, false]),
            "update" => Some([true, true]),
            _ => None,
        }
    }

    /// The name a key component stands for, in `encoding`.
    fn name_from_text(encoding: NameEncoding, text: &str) -> Vec<u8> {
        match encoding {
            NameEncoding::Utf8 | NameEncoding::PosixBytes => text.as_bytes().to_vec(),
            NameEncoding::WindowsUtf16Le => {
                text.encode_utf16().flat_map(u16::to_le_bytes).collect()
            }
        }
    }

    struct Reader<'a>(&'a [u8]);
    impl<'a> Reader<'a> {
        fn take(&mut self, count: usize) -> Result<&'a [u8], Malformed> {
            let (first, rest) = self.0.split_at_checked(count).ok_or(Malformed)?;
            self.0 = rest;
            Ok(first)
        }
        fn byte(&mut self) -> Result<u8, Malformed> {
            self.take(1)?.first().copied().ok_or(Malformed)
        }
        fn number(&mut self) -> Result<usize, Malformed> {
            let before = self.0.len();
            let value = prost::encoding::decode_varint(&mut self.0).map_err(|_| Malformed)?;
            if prost::encoding::encoded_len_varint(value) != before - self.0.len() {
                return Err(Malformed);
            }
            usize::try_from(value).map_err(|_| Malformed)
        }
    }

    pub(super) fn entry(
        key: &[u8],
        operation: &str,
        metadata: &[u8],
        config: VolumeConfig,
    ) -> Result<PublicationEntry, Malformed> {
        if operation == "publish" && metadata.is_empty() && key.is_empty() {
            return Ok(PublicationEntry {
                path: NamespacePath::new(vec![], config.limits).map_err(|_| Malformed)?,
                before_present: false,
                after_present: false,
            });
        }
        let expected = presence(operation).ok_or(Malformed)?;
        let mut reader = Reader(metadata);
        let maximum = config.limits.maximum_component_bytes;
        let name = |encoding: NameEncoding, bytes: &[u8]| {
            if bytes.len() > maximum as usize {
                return Err(Malformed);
            }
            LogicalName::new(encoding, bytes.to_vec(), maximum).map_err(|_| Malformed)
        };
        let mut components = Vec::new();
        match reader.number()? {
            0 => {
                let encoding = NameEncoding::from_tag(reader.byte()?).map_err(|_| Malformed)?;
                let text = std::str::from_utf8(key).map_err(|_| Malformed)?;
                if config.case_sensitivity == CaseSensitivity::ProfileFolded
                    || key.is_empty()
                    || key.len() >= acyclic_stream::MAX_RECORD_KEY_BYTES
                    || text.split('/').count() > config.limits.maximum_path_depth as usize
                {
                    return Err(Malformed);
                }
                for part in text.split('/') {
                    components.push(name(encoding, &name_from_text(encoding, part))?);
                }
            }
            depth => {
                let depth = depth - 1;
                if depth > config.limits.maximum_path_depth as usize || depth > reader.0.len() / 2 {
                    return Err(Malformed);
                }
                for _ in 0..depth {
                    let encoding = NameEncoding::from_tag(reader.byte()?).map_err(|_| Malformed)?;
                    let length = reader.number()?;
                    components.push(name(encoding, reader.take(length)?)?);
                }
            }
        }
        for present in expected {
            if present {
                reader.take(16)?;
                FileKind::from_tag(reader.byte()?).map_err(|_| Malformed)?;
                reader.take(32)?;
            }
        }
        if !reader.0.is_empty() {
            return Err(Malformed);
        }
        Ok(PublicationEntry {
            path: NamespacePath::new(components, config.limits).map_err(|_| Malformed)?,
            before_present: expected[0],
            after_present: expected[1],
        })
    }

    #[cfg(test)]
    pub(crate) fn path(
        key: &[u8],
        operation: &str,
        metadata: &[u8],
        config: VolumeConfig,
    ) -> Result<NamespacePath, Malformed> {
        entry(key, operation, metadata, config).map(|entry| entry.path)
    }
}
#[cfg(test)]
pub(crate) use read::path;

#[cfg(test)]
mod tests;
