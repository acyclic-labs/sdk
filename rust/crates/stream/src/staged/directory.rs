use super::*;
use crate::content::index::{Index, Sorter};

const MAGIC: &[u8] = b"acyclic-stream-mutations-v1\0";
const ROOT_BYTES: usize = 41;
const MANIFEST_BYTES: usize = MAGIC.len() + 2 * ROOT_BYTES;
const LOCATION_BYTES: usize = 16 + super::decode::BOOKMARK_BYTES;

/// Private immutable destination index over an original staged token body.
/// This is not an authority decision, namespace root or authorization credential.
#[derive(Clone)]
pub struct MutationDirectory {
    staged: StagedWrite,
    destinations: Index,
    root: Root,
}

/// Original caller ordinals and the verified boundary before one mutation.
pub struct MutationLocation {
    /// Zero-based effect ordinal in caller order.
    pub effect: u64,
    /// Zero-based mutation ordinal within its original effect.
    pub mutation: u64,
    bookmark: Bookmark,
}

/// Lazy byte-ordered destinations of one pinned private directory.
pub struct MutationCursor<'a, S> {
    staged: StagedWrite,
    cursor: crate::content::index::Cursor<'a, S>,
    interrupted: bool,
}

fn location(staged: &StagedWrite, value: Field) -> Result<MutationLocation, StreamError> {
    let Field::Inline(bytes) = value else {
        return Err(StreamError::InvalidResponse);
    };
    let bytes = bytes.as_slice();
    if bytes.len() != LOCATION_BYTES {
        return Err(StreamError::InvalidResponse);
    }
    Ok(MutationLocation {
        effect: u64::from_le_bytes(
            bytes[..8]
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
        ),
        mutation: u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
        ),
        bookmark: Bookmark::decode(staged, &bytes[16..])?,
    })
}

impl<S: Storage> MutationCursor<'_, S> {
    /// Returns the next original mutation boundary in destination order.
    pub async fn next(&mut self) -> Result<Option<(Field, MutationLocation)>, StreamError> {
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

impl MutationLocation {
    /// Resumes at the mutation descriptor without reading the body prefix.
    pub fn reader<'a, S: Storage>(
        &self,
        storage: &'a S,
        staged: &StagedWrite,
    ) -> Result<Reader<'a, S>, StreamError> {
        Reader::resume(storage, staged, &self.bookmark)
    }
}

impl MutationDirectory {
    /// Derives the unique destination index with bounded external ordering.
    /// Successful writes remain retained by the enclosing stage on cancellation.
    pub async fn build(storage: &impl Storage, staged: &StagedWrite) -> Result<Self, StreamError> {
        let mut reader = Reader::open(storage, staged).await?;
        let mut sorter = Sorter::new(storage);
        let mut effects = 0_u64;
        let mut mutation = 0_u64;
        loop {
            let bookmark = reader.bookmark()?;
            let Some(token) = reader.next().await? else {
                break;
            };
            let destination = match token {
                Token::Effect { .. } => {
                    effects = effects.checked_add(1).ok_or(StreamError::Capacity)?;
                    mutation = 0;
                    continue;
                }
                Token::Append { path, .. } => path,
                Token::Fork { destination, .. } => destination,
                _ => continue,
            };
            let effect = effects.checked_sub(1).ok_or(StreamError::InvalidResponse)?;
            let mut value = [0; LOCATION_BYTES];
            value[..8].copy_from_slice(&effect.to_le_bytes());
            value[8..16].copy_from_slice(&mutation.to_le_bytes());
            value[16..].copy_from_slice(&bookmark.encode());
            sorter
                .push(
                    destination,
                    Field::Inline(OwnedBytes::copy_from_slice(&value)),
                )
                .await?;
            mutation = mutation.checked_add(1).ok_or(StreamError::Capacity)?;
        }
        let destinations = sorter.finish().await?;
        let checkpoint = destinations.checkpoint(storage).await?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MANIFEST_BYTES)
            .map_err(|_| StreamError::Capacity)?;
        bytes.extend_from_slice(MAGIC);
        for root in [staged.body, checkpoint] {
            bytes.extend_from_slice(&root.part.digest);
            bytes.extend_from_slice(&root.part.length.to_le_bytes());
            bytes.push(root.height);
        }
        let mut writer = tree::Writer::new(storage)?;
        writer.reference(staged.body).await?;
        writer.reference(checkpoint).await?;
        writer.write(&bytes).await?;
        let root = writer.finish().await?;
        Ok(Self {
            staged: staged.clone(),
            destinations,
            root,
        })
    }

    /// Retains both the original body and every destination-index dependency.
    /// This root must remain private until the enclosing authority publishes.
    pub const fn root(&self) -> Root {
        self.root
    }

    /// Scans descriptors in destination order without changing body order.
    pub async fn scan<'a, S: Storage>(
        &self,
        storage: &'a S,
    ) -> Result<MutationCursor<'a, S>, StreamError> {
        Ok(MutationCursor {
            staged: self.staged.clone(),
            cursor: self.destinations.scan(storage, None).await?,
            interrupted: false,
        })
    }

    /// Opens an authenticated directory bound to the supplied original stage.
    /// Integrity checks do not grant authority to supply arbitrary bookmarks.
    pub async fn open(
        storage: &impl Storage,
        staged: &StagedWrite,
        root: Root,
    ) -> Result<Self, StreamError> {
        if root.part.length != MANIFEST_BYTES as u64 {
            return Err(StreamError::InvalidResponse);
        }
        let mut reader = tree::Reader::new(storage, root)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(MANIFEST_BYTES)
            .map_err(|_| StreamError::Capacity)?;
        while let Some(page) = reader.next().await? {
            bytes.extend_from_slice(page.as_slice());
        }
        let bytes = crate::persistence::header(&bytes, b"acyclic-stream-mutations-v", b"1\0")?;
        if bytes.len() != 2 * ROOT_BYTES {
            return Err(StreamError::InvalidResponse);
        }
        let decode = |bytes: &[u8]| -> Result<Root, StreamError> {
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
        };
        if decode(&bytes[..ROOT_BYTES])? != staged.body {
            return Err(StreamError::InvalidArgument);
        }
        // Verify the supplied id/key/family against the body, not only its root.
        Reader::open(storage, staged).await?;
        let destinations = Index::open(storage, decode(&bytes[ROOT_BYTES..])?).await?;
        Ok(Self {
            staged: staged.clone(),
            destinations,
            root,
        })
    }

    /// Looks up a destination by logical path bytes, independent of representation.
    pub async fn get(
        &self,
        storage: &impl Storage,
        destination: Field,
    ) -> Result<Option<MutationLocation>, StreamError> {
        let Some(value) = self.destinations.get(storage, destination).await? else {
            return Ok(None);
        };
        Ok(Some(location(&self.staged, value)?))
    }
}
