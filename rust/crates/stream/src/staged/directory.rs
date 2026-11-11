use super::*;
use crate::content::index::{Index, Sorter};

const MAGIC: &[u8] = b"acyclic-stream-directory-v1\0";
pub(super) const ROOT_BYTES: usize = 41;
const MANIFEST_BYTES: usize = MAGIC.len() + 3 * ROOT_BYTES;
const LOCATION_BYTES: usize = 16 + super::decode::BOOKMARK_BYTES;

/// Private mutation and effect/condition indexes over one original token body.
/// This is not an authority decision, namespace root or authorization credential.
#[derive(Clone)]
pub struct Directory {
    staged: StagedWrite,
    destinations: Index,
    effects: Index,
    root: Root,
}

/// Original caller ordinals and a verified condition or mutation boundary.
pub struct Location {
    /// Zero-based effect ordinal in caller order.
    pub effect: u64,
    /// Zero-based condition or mutation ordinal within its original effect.
    pub ordinal: u64,
    bookmark: Bookmark,
}

/// Lazy byte-ordered destinations of one pinned private directory.
pub struct Cursor<'a, S> {
    staged: StagedWrite,
    cursor: crate::content::index::Cursor<'a, S>,
    interrupted: bool,
}

pub(super) fn location(staged: &StagedWrite, value: Field) -> Result<Location, StreamError> {
    let Field::Inline(bytes) = value else {
        return Err(StreamError::InvalidResponse);
    };
    let bytes = bytes.as_slice();
    if bytes.len() != LOCATION_BYTES {
        return Err(StreamError::InvalidResponse);
    }
    Ok(Location {
        effect: u64::from_le_bytes(
            bytes[..8]
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
        ),
        ordinal: u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
        ),
        bookmark: Bookmark::decode(staged, &bytes[16..])?,
    })
}

pub(super) fn row(effect: u64, mutation: u64, bookmark: &Bookmark) -> Field {
    let mut value = [0; LOCATION_BYTES];
    value[..8].copy_from_slice(&effect.to_le_bytes());
    value[8..16].copy_from_slice(&mutation.to_le_bytes());
    value[16..].copy_from_slice(&bookmark.encode());
    Field::Inline(OwnedBytes::copy_from_slice(&value))
}

pub(super) fn encode_root(bytes: &mut Vec<u8>, root: Root) {
    bytes.extend_from_slice(&root.part.digest);
    bytes.extend_from_slice(&root.part.length.to_le_bytes());
    bytes.push(root.height);
}

pub(super) fn decode_root(bytes: &[u8]) -> Result<Root, StreamError> {
    if bytes.len() != ROOT_BYTES || bytes[40] > u64::BITS as u8 {
        return Err(StreamError::InvalidResponse);
    }
    Ok(Root {
        part: crate::content::Part {
            digest: bytes[..32]
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
            length: u64::from_le_bytes(
                bytes[32..40]
                    .try_into()
                    .map_err(|_| StreamError::InvalidResponse)?,
            ),
        },
        height: bytes[40],
    })
}

pub(super) async fn body(
    storage: &impl Storage,
    root: Root,
    length: usize,
) -> Result<OwnedBytes, StreamError> {
    if root.part.length != length as u64 {
        return Err(StreamError::InvalidResponse);
    }
    let mut reader = tree::Reader::new(storage, root)?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(length)
        .map_err(|_| StreamError::Capacity)?;
    while let Some(page) = reader.next().await? {
        bytes.extend_from_slice(page.as_slice());
    }
    Ok(bytes.into())
}

impl<'a, S: Storage> Cursor<'a, S> {
    pub(super) async fn open(
        storage: &'a S,
        staged: &StagedWrite,
        index: &Index,
    ) -> Result<Self, StreamError> {
        Ok(Self {
            staged: staged.clone(),
            cursor: index.scan(storage, None).await?,
            interrupted: false,
        })
    }
}

impl<S: Storage> Cursor<'_, S> {
    /// Returns the next original boundary in byte-path order.
    pub async fn next(&mut self) -> Result<Option<(Field, Location)>, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        self.interrupted = true;
        let entry = match self.cursor.next().await? {
            Some((key, value)) => Some((key, location(&self.staged, value)?)),
            None => None,
        };
        self.interrupted = false;
        Ok(entry)
    }
}

impl Location {
    /// Resumes at the original descriptor without reading the body prefix.
    pub fn reader<'a, S: Storage>(
        &self,
        storage: &'a S,
        staged: &StagedWrite,
    ) -> Result<Reader<'a, S>, StreamError> {
        Reader::resume(storage, staged, &self.bookmark)
    }
}

impl Directory {
    /// Derives the unique destination index with bounded external ordering.
    /// Successful writes remain retained by the enclosing stage on cancellation.
    pub async fn build(storage: &impl Storage, staged: &StagedWrite) -> Result<Self, StreamError> {
        let mut reader = Reader::open(storage, staged).await?;
        let mut sorter = Sorter::new(storage);
        let mut effect_index = crate::content::index::Builder::new(storage);
        let mut pending: Option<super::effects::Pending<'_, _>> = None;
        let mut effects = 0_u64;
        loop {
            let bookmark = reader.bookmark()?;
            let Some(token) = reader.next().await? else {
                break;
            };
            let destination = match token {
                Token::Effect { soft, .. } => {
                    if let Some(previous) = pending.take() {
                        previous.finish(storage, &mut effect_index).await?;
                    }
                    pending = Some(super::effects::Pending::new(
                        storage, effects, soft, bookmark,
                    ));
                    effects = effects.checked_add(1).ok_or(StreamError::Capacity)?;
                    continue;
                }
                Token::Presence { path, .. } | Token::Tail { path, .. } => {
                    let current = pending.as_mut().ok_or(StreamError::InvalidResponse)?;
                    current.condition(path, &bookmark).await?;
                    continue;
                }
                Token::Append { path, .. } => path,
                Token::Fork { destination, .. } => destination,
                _ => continue,
            };
            let current = pending.as_mut().ok_or(StreamError::InvalidResponse)?;
            sorter
                .push(
                    destination,
                    row(current.ordinal, current.mutation, &bookmark),
                )
                .await?;
            current.mutation = current
                .mutation
                .checked_add(1)
                .ok_or(StreamError::Capacity)?;
        }
        if let Some(previous) = pending {
            previous.finish(storage, &mut effect_index).await?;
        }
        let effects = effect_index.finish().await?;
        let effect_checkpoint = effects.checkpoint(storage).await?;
        let destinations = sorter.finish().await?;
        let checkpoint = destinations.checkpoint(storage).await?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MANIFEST_BYTES)
            .map_err(|_| StreamError::Capacity)?;
        bytes.extend_from_slice(MAGIC);
        for root in [staged.body, checkpoint, effect_checkpoint] {
            encode_root(&mut bytes, root);
        }
        let mut writer = tree::Writer::new(storage)?;
        writer.reference(staged.body).await?;
        writer.reference(checkpoint).await?;
        writer.reference(effect_checkpoint).await?;
        writer.write(&bytes).await?;
        let root = writer.finish().await?;
        Ok(Self {
            staged: staged.clone(),
            destinations,
            effects,
            root,
        })
    }

    /// Retains both the original body and every destination-index dependency.
    /// This root must remain private until the enclosing authority publishes.
    pub const fn root(&self) -> Root {
        self.root
    }

    /// Scans descriptors in destination order without changing body order.
    pub async fn scan<'a, S: Storage>(&self, storage: &'a S) -> Result<Cursor<'a, S>, StreamError> {
        Cursor::open(storage, &self.staged, &self.destinations).await
    }

    /// Opens effects in original caller order; conditions remain unevaluated.
    pub async fn effects<'a, S: Storage>(
        &self,
        storage: &'a S,
    ) -> Result<Effects<'a, S>, StreamError> {
        Effects::open(storage, &self.staged, &self.effects).await
    }

    /// Opens an authenticated directory bound to the supplied original stage.
    /// Integrity checks do not grant authority to supply arbitrary bookmarks.
    pub async fn open(
        storage: &impl Storage,
        staged: &StagedWrite,
        root: Root,
    ) -> Result<Self, StreamError> {
        let bytes = body(storage, root, MANIFEST_BYTES).await?;
        let bytes =
            crate::persistence::header(bytes.as_slice(), b"acyclic-stream-directory-v", b"1\0")?;
        if bytes.len() != 3 * ROOT_BYTES {
            return Err(StreamError::InvalidResponse);
        }
        if decode_root(&bytes[..ROOT_BYTES])? != staged.body {
            return Err(StreamError::InvalidArgument);
        }
        // Verify the supplied id/key/family against the body, not only its root.
        Reader::open(storage, staged).await?;
        let destinations =
            Index::open(storage, decode_root(&bytes[ROOT_BYTES..2 * ROOT_BYTES])?).await?;
        let effects = Index::open(storage, decode_root(&bytes[2 * ROOT_BYTES..])?).await?;
        Ok(Self {
            staged: staged.clone(),
            destinations,
            effects,
            root,
        })
    }

    /// Looks up a destination by logical path bytes, independent of representation.
    pub async fn get(
        &self,
        storage: &impl Storage,
        destination: Field,
    ) -> Result<Option<Location>, StreamError> {
        let Some(value) = self.destinations.get(storage, destination).await? else {
            return Ok(None);
        };
        Ok(Some(location(&self.staged, value)?))
    }
}
