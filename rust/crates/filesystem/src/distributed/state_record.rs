//! Portable v1 data records. Placement, writer ownership and commit identity
//! belong to Streams, never to immutable filesystem tree bytes.

use crate::{FileId, ObjectId, ObjectKind};
use acyclic_stream::OwnedBytes as Bytes;

const DOMAIN: &[u8] = b"acyclic-fs-state-v1\0";

/// Retry identity and typed state retained together in an ordinary record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RecordedState {
    /// Original caller key; its lifetime follows record retention.
    pub operation: acyclic_stream::IdempotencyKey,
    /// Original append destination, retained unchanged when a subtree inherits
    /// this record. This is publication provenance, not immutable tree identity.
    pub published_at: Bytes,
    /// Published state or permanent semantic outcome.
    pub state: StateRecord,
}

/// A portable immutable tree, shared unchanged by an unfiltered subtree fork.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PortableRoot {
    /// Stable directory identity, independent of its destination volume.
    pub root_file_id: FileId,
    /// Authenticated immutable file table.
    pub file_table: ObjectId,
}

impl PortableRoot {
    fn is_valid(self) -> bool {
        self.file_table.kind == ObjectKind::FileTablePage
            && self
                .file_table
                .storage
                .is_none_or(|storage| storage.root_file_id == self.root_file_id)
    }
}

impl From<&crate::kernel::GenerationRoot> for PortableRoot {
    fn from(root: &crate::kernel::GenerationRoot) -> Self {
        Self {
            root_file_id: root.root_file_id,
            file_table: root.file_table,
        }
    }
}

/// Companion codec selected by the caller; payloads use the existing typed
/// context, lazy-workspace or Git-state codec, rather than a second journal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CompanionKind {
    /// Recursive workspace bindings.
    WorkspaceContext,
    /// Sparse overlays and source-identity shadows.
    SparseWorkspace,
    /// Git refs and command outcomes.
    GitRefs,
    /// Permanent semantic operation outcome, retained as ordinary data.
    OperationOutcome,
}

/// One typed record on an existing Stream. A volume begins with one stream;
/// its root and companion snapshot change in the same record and commit.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StateRecord {
    /// Complete volume root plus its encoded companion snapshot.
    Volume {
        /// Portable immutable tree.
        root: PortableRoot,
        /// Existing companion codec bytes, including overlay/shadow roots.
        companion: Bytes,
    },
    /// A companion record composed with a volume or actor publication.
    Companion {
        /// Selects the existing payload codec.
        kind: CompanionKind,
        /// Canonical bytes of the selected codec.
        payload: Bytes,
    },
}

impl StateRecord {
    pub(super) fn fields(
        &self,
    ) -> Result<(u8, Option<PortableRoot>, &[u8]), crate::AuthorityStoreError> {
        match self {
            Self::Volume { root, companion } => {
                if !root.is_valid() {
                    return Err(crate::AuthorityStoreError::RejectedStatic(
                        "volume root requires a file table in its root lineage",
                    ));
                }
                Ok((0, Some(*root), companion))
            }
            Self::Companion { kind, payload } => Ok((
                match kind {
                    CompanionKind::WorkspaceContext => 1,
                    CompanionKind::SparseWorkspace => 2,
                    CompanionKind::GitRefs => 3,
                    CompanionKind::OperationOutcome => 4,
                },
                None,
                payload,
            )),
        }
    }

    /// Decodes framing without copying its retained provider bytes. The caller
    /// decodes the selected companion payload through its existing typed codec.
    pub fn decode(bytes: Bytes) -> Result<RecordedState, crate::AuthorityStoreError> {
        let corrupt =
            || crate::AuthorityStoreError::CorruptStatic("invalid filesystem state record");
        if bytes.len() > acyclic_stream::MAX_RECORD_BYTES || !bytes.starts_with(DOMAIN) {
            return Err(corrupt());
        }
        let key_start = DOMAIN.len() + 2;
        let key_length = u16::from_le_bytes(
            bytes
                .get(DOMAIN.len()..key_start)
                .ok_or_else(corrupt)?
                .try_into()
                .map_err(|_| corrupt())?,
        ) as usize;
        let key_end = key_start.checked_add(key_length).ok_or_else(corrupt)?;
        let path_start = key_end.checked_add(2).ok_or_else(corrupt)?;
        let path_length = u16::from_le_bytes(
            bytes
                .get(key_end..path_start)
                .ok_or_else(corrupt)?
                .try_into()
                .map_err(|_| corrupt())?,
        ) as usize;
        if path_length == 0 || path_length > acyclic_stream::MAX_PATH_BYTES {
            return Err(corrupt());
        }
        let tag_index = path_start.checked_add(path_length).ok_or_else(corrupt)?;
        let tag = *bytes.get(tag_index).ok_or_else(corrupt)?;
        let operation = acyclic_stream::IdempotencyKey::new(bytes.slice(key_start..key_end))
            .map_err(|_| corrupt())?;
        let published_at = bytes.slice(path_start..tag_index);
        let start = tag_index + 1;
        let state = if tag == 0 {
            let root_file_id = FileId::from_bytes(
                bytes
                    .get(start..start + 16)
                    .ok_or_else(corrupt)?
                    .try_into()
                    .map_err(|_| corrupt())?,
            );
            let reference = start + 16;
            let length = if bytes.get(reference).ok_or_else(corrupt)? & 128 == 0 {
                33
            } else {
                ObjectId::ENCODED_LEN
            };
            let end = reference + length;
            let file_table = ObjectId::try_from(bytes.get(reference..end).ok_or_else(corrupt)?)
                .map_err(|_| corrupt())?;
            let root = PortableRoot {
                root_file_id,
                file_table,
            };
            if !root.is_valid() {
                return Err(corrupt());
            }
            Self::Volume {
                root,
                companion: bytes.slice(end..),
            }
        } else {
            let kind = match tag {
                1 => CompanionKind::WorkspaceContext,
                2 => CompanionKind::SparseWorkspace,
                3 => CompanionKind::GitRefs,
                4 => CompanionKind::OperationOutcome,
                _ => return Err(corrupt()),
            };
            Self::Companion {
                kind,
                payload: bytes.slice(start..),
            }
        };
        Ok(RecordedState {
            operation,
            published_at,
            state,
        })
    }
}

impl crate::AuthorityWork<'_> {
    pub(super) fn encode_state(
        &mut self,
        record: &StateRecord,
        operation: &acyclic_stream::IdempotencyKey,
        path: &acyclic_stream::StreamPath,
    ) -> Result<acyclic_stream::OwnedBytes, crate::AuthorityStoreError> {
        let (tag, root, payload) = record.fields()?;
        let length = DOMAIN
            .len()
            .checked_add(
                5 + operation.as_bytes().len()
                    + path.as_str().len()
                    + root.map_or(0, |root| 16 + root.file_table.encoded_len()),
            )
            .and_then(|n| n.checked_add(payload.len()))
            .ok_or(crate::WorkError::Overflow)?;
        if length > acyclic_stream::MAX_RECORD_BYTES {
            return Err(acyclic_stream::StreamError::LimitExceeded.into());
        }
        let tag = [tag];
        let key_length = u16::try_from(operation.as_bytes().len())
            .map_err(|_| crate::WorkError::Overflow)?
            .to_le_bytes();
        let path_length = u16::try_from(path.as_str().len())
            .map_err(|_| crate::WorkError::Overflow)?
            .to_le_bytes();
        match root {
            Some(root) => self.stream_bytes(
                DOMAIN,
                &[
                    &key_length,
                    operation.as_bytes(),
                    &path_length,
                    path.as_str().as_bytes(),
                    &tag,
                    &root.root_file_id.into_bytes(),
                    &root.file_table.to_bytes(),
                    payload,
                ],
            ),
            None => self.stream_bytes(
                DOMAIN,
                &[
                    &key_length,
                    operation.as_bytes(),
                    &path_length,
                    path.as_str().as_bytes(),
                    &tag,
                    payload,
                ],
            ),
        }
    }
}
