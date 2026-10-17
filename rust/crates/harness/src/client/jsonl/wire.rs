//! Wire forms of the task-volume values the JSONL dialect carries. Each one
//! differs from its task-volume type only in byte fields, which use the one
//! canonical byte encoding. Conversions destructure every field, so a field
//! added to a task-volume type cannot be silently left off the wire.
use super::bytes;
use crate::{
    OperationId,
    task_volume::{
        Change, Channel, Cursor, FileKey, FileValue, Generation, HeadFile, HeadSnapshot,
    },
};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct File<'a> {
    key: Cow<'a, FileKey>,
    channel: Option<Channel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    requester: Option<crate::TaskId>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    publication: Option<OperationId>,
    #[serde(with = "bytes")]
    bytes: Cow<'a, [u8]>,
}

impl<'a> From<&'a FileValue> for File<'a> {
    fn from(file: &'a FileValue) -> Self {
        let FileValue {
            key,
            channel,
            requester,
            publication,
            bytes,
        } = file;
        Self {
            key: Cow::Borrowed(key),
            channel: *channel,
            requester: *requester,
            publication: *publication,
            bytes: Cow::Borrowed(bytes.as_slice()),
        }
    }
}

impl From<File<'_>> for FileValue {
    fn from(file: File<'_>) -> Self {
        let File {
            key,
            channel,
            requester,
            publication,
            bytes,
        } = file;
        Self {
            key: key.into_owned(),
            channel,
            requester,
            publication,
            bytes: bytes.into_owned().into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CursorForm {
    #[serde(with = "bytes::digest")]
    volume: [u8; 32],
    epoch: uuid::Uuid,
    schema: u32,
    #[serde(with = "bytes::digest")]
    query: [u8; 32],
    position: u64,
    accepted: u64,
    durable: u64,
    #[serde(with = "bytes::digest")]
    commit: [u8; 32],
}

impl From<&Cursor> for CursorForm {
    fn from(cursor: &Cursor) -> Self {
        let Cursor {
            volume,
            epoch,
            schema,
            query,
            position,
            accepted,
            durable,
            commit,
        } = *cursor;
        Self {
            volume,
            epoch,
            schema,
            query,
            position,
            accepted,
            durable,
            commit,
        }
    }
}

impl From<CursorForm> for Cursor {
    fn from(cursor: CursorForm) -> Self {
        let CursorForm {
            volume,
            epoch,
            schema,
            query,
            position,
            accepted,
            durable,
            commit,
        } = cursor;
        Self {
            volume,
            epoch,
            schema,
            query,
            position,
            accepted,
            durable,
            commit,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct GenerationForm {
    #[serde(with = "bytes::digest")]
    volume: [u8; 32],
    epoch: uuid::Uuid,
    position: u64,
    #[serde(with = "bytes::digest")]
    commit: [u8; 32],
}

impl From<&Generation> for GenerationForm {
    fn from(generation: &Generation) -> Self {
        let Generation {
            volume,
            epoch,
            position,
            commit,
        } = *generation;
        Self {
            volume,
            epoch,
            position,
            commit,
        }
    }
}

impl From<GenerationForm> for Generation {
    fn from(generation: GenerationForm) -> Self {
        let GenerationForm {
            volume,
            epoch,
            position,
            commit,
        } = generation;
        Self {
            volume,
            epoch,
            position,
            commit,
        }
    }
}

/// A materialized head is one logical file: its segments travel as the single
/// byte value they spell, and its length is that value's length. A contiguous
/// head is borrowed; only a fragmented head is joined.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Head<'a> {
    key: Cow<'a, FileKey>,
    channel: Option<Channel>,
    publication: Option<OperationId>,
    #[serde(with = "bytes")]
    bytes: Cow<'a, [u8]>,
    through_sequence: Option<u64>,
}

impl<'a> From<&'a HeadFile> for Head<'a> {
    fn from(file: &'a HeadFile) -> Self {
        let HeadFile {
            key,
            channel,
            publication,
            segments,
            length: _,
            through_sequence,
        } = file;
        Self {
            key: Cow::Borrowed(key),
            channel: *channel,
            publication: *publication,
            bytes: match segments.as_slice() {
                [] => Cow::Borrowed(&[]),
                [segment] => Cow::Borrowed(segment.as_slice()),
                fragments => Cow::Owned(
                    fragments
                        .iter()
                        .map(|segment| segment.as_slice())
                        .collect::<Vec<_>>()
                        .concat(),
                ),
            },
            through_sequence: *through_sequence,
        }
    }
}

impl From<Head<'_>> for HeadFile {
    fn from(file: Head<'_>) -> Self {
        let Head {
            key,
            channel,
            publication,
            bytes,
            through_sequence,
        } = file;
        Self {
            key: key.into_owned(),
            channel,
            publication,
            length: bytes.len() as u64,
            segments: vec![bytes.into_owned().into()],
            through_sequence,
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Heads<'a> {
    files: Vec<Head<'a>>,
    cursor: CursorForm,
    generation: GenerationForm,
}

impl<'a> From<&'a HeadSnapshot> for Heads<'a> {
    fn from(head: &'a HeadSnapshot) -> Self {
        let HeadSnapshot {
            files,
            cursor,
            generation,
        } = head;
        Self {
            files: files.iter().map(Head::from).collect(),
            cursor: cursor.into(),
            generation: generation.into(),
        }
    }
}

impl From<Heads<'_>> for HeadSnapshot {
    fn from(head: Heads<'_>) -> Self {
        let Heads {
            files,
            cursor,
            generation,
        } = head;
        Self {
            files: files.into_iter().map(HeadFile::from).collect(),
            cursor: cursor.into(),
            generation: generation.into(),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct ChangeForm<'a> {
    file: Option<File<'a>>,
    files: Vec<File<'a>>,
    removed: Cow<'a, [FileKey]>,
    cursor: CursorForm,
}

impl<'a> From<&'a Change> for ChangeForm<'a> {
    fn from(change: &'a Change) -> Self {
        let Change {
            file,
            files,
            removed,
            cursor,
        } = change;
        Self {
            file: file.as_ref().map(File::from),
            files: files.iter().map(File::from).collect(),
            removed: Cow::Borrowed(removed),
            cursor: cursor.into(),
        }
    }
}

impl From<ChangeForm<'_>> for Change {
    fn from(change: ChangeForm<'_>) -> Self {
        let ChangeForm {
            file,
            files,
            removed,
            cursor,
        } = change;
        Self {
            file: file.map(FileValue::from),
            files: files.into_iter().map(FileValue::from).collect(),
            removed: removed.into_owned(),
            cursor: cursor.into(),
        }
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::Head;
    use crate::task_volume::{FileKey, HeadFile};
    use std::borrow::Cow;

    fn head(segments: &[&[u8]]) -> HeadFile {
        HeadFile {
            key: FileKey::parse("commands/head.json").unwrap(),
            channel: None,
            publication: None,
            segments: segments
                .iter()
                .map(|segment| segment.to_vec().into())
                .collect(),
            length: segments.iter().map(|segment| segment.len() as u64).sum(),
            through_sequence: None,
        }
    }

    // A contiguous head is spelled from its own storage: no allocation and the
    // very same bytes. The negative control is a fragmented head, which must
    // be joined into one owned value of the full length.
    #[test]
    fn contiguous_heads_are_borrowed_and_only_fragments_are_joined() {
        let payload = vec![b'x'; 1 << 16];
        let contiguous = head(&[&payload]);
        let mut wire = None;
        let allocated = acyclic_test_support::measure(|| wire = Some(Head::from(&contiguous)));
        let wire = wire.unwrap();
        assert_eq!(allocated.bytes_total, 0);
        assert!(
            matches!(&wire.bytes, Cow::Borrowed(bytes) if bytes.as_ptr() == contiguous.segments[0].as_ptr())
        );

        let fragmented = head(&[&payload[..1], &payload[1..]]);
        let mut wire = None;
        let allocated = acyclic_test_support::measure(|| wire = Some(Head::from(&fragmented)));
        let wire = wire.unwrap();
        assert!(allocated.bytes_total >= payload.len() as u64);
        assert!(matches!(&wire.bytes, Cow::Owned(bytes) if *bytes == payload));
    }
}
