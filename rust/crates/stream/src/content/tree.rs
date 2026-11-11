//! Incremental fixed-page content trees over the shared Objects manifest codec.
use super::{Manifest, Part, encode_manifest, verify};
use crate::{OwnedBytes, ProviderPlatform, StreamError, content_digest};
use async_trait::async_trait;
use futures::{Stream, StreamExt};

/// Physical page size, independent of logical record or commit size.
pub const PAGE_BYTES: usize = 64 * 1024;
const BLOB_MAGIC: &[u8] = b"acyclic-content-leaf-v1\0";
const LEAF_BYTES: usize = PAGE_BYTES - BLOB_MAGIC.len();
const FANOUT: usize = (PAGE_BYTES - super::MANIFEST_MAGIC.len() - 8) / 40;
pub(crate) const EDGE_FANOUT: usize = (PAGE_BYTES - super::MANIFEST_MAGIC.len() - 8) / 32;
// Even binary fanout cannot require more levels for a u64 logical length.
const MAX_HEIGHT: u8 = u64::BITS as u8;

/// Stored page interpretation, retained separately from arbitrary blob bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Kind {
    /// Logical bytes.
    Blob,
    /// A canonical manifest naming lower-level pages.
    Manifest,
}

/// One page returned with its original payload ownership.
pub struct Page {
    /// Stored interpretation; arbitrary blob bytes are never sniffed for a header.
    pub kind: Kind,
    /// At most one physical page of bytes.
    pub bytes: OwnedBytes,
}

/// The small immutable reference retained by a record or staged manifest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Root {
    /// Stored bytes digest and total logical length.
    pub part: Part,
    /// Number of manifest levels above blob leaves.
    pub height: u8,
}

/// Backend page IO only; traversal, splitting, hashing and encoding are shared.
///
/// The enclosing stage owns each successful put until publication or retirement.
/// Backends must retain immutable pages and reject inconsistent kind/bytes for
/// an existing digest. Reads retain their original allocation/transport claims.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait Storage: ProviderPlatform {
    /// Stores one physical page in the caller's stage; never publishes a record.
    async fn put(&self, digest: [u8; 32], page: Page) -> Result<(), StreamError>;
    /// Reads exactly one immutable stored page, retaining its owner.
    async fn get(&self, digest: [u8; 32]) -> Result<Page, StreamError>;
}

async fn put(
    storage: &impl Storage,
    kind: Kind,
    bytes: Vec<u8>,
    length: u64,
) -> Result<Part, StreamError> {
    let digest = content_digest(&bytes);
    storage
        .put(
            digest,
            Page {
                kind,
                bytes: bytes.into(),
            },
        )
        .await?;
    Ok(Part { digest, length })
}

trait TreeEntry: Copy {
    const FANOUT: usize;
    fn encode(entries: &[Self]) -> Result<(Vec<u8>, u64), StreamError>;
    fn parent(part: Part) -> Self;
}

impl TreeEntry for Part {
    const FANOUT: usize = self::FANOUT;
    fn encode(entries: &[Self]) -> Result<(Vec<u8>, u64), StreamError> {
        let length = entries.iter().try_fold(0_u64, |sum, part| {
            sum.checked_add(part.length).ok_or(StreamError::Capacity)
        })?;
        Ok((
            encode_manifest(entries.iter().copied(), std::iter::empty())?,
            length,
        ))
    }
    fn parent(part: Part) -> Self {
        part
    }
}

#[derive(Clone, Copy)]
struct Edge([u8; 32]);

impl TreeEntry for Edge {
    const FANOUT: usize = EDGE_FANOUT;
    fn encode(entries: &[Self]) -> Result<(Vec<u8>, u64), StreamError> {
        Ok((
            encode_manifest(std::iter::empty(), entries.iter().map(|edge| edge.0))?,
            0,
        ))
    }
    fn parent(part: Part) -> Self {
        Self(part.digest)
    }
}

struct Builder<'a, S, T = Part> {
    storage: &'a S,
    levels: Vec<Vec<T>>,
}

impl<S: Storage, T: TreeEntry> Builder<'_, S, T> {
    async fn node(&self, entries: &[T]) -> Result<T, StreamError> {
        let (bytes, length) = T::encode(entries)?;
        let part = put(self.storage, Kind::Manifest, bytes, length).await?;
        crate::runtime::yield_now().await?;
        Ok(T::parent(part))
    }

    async fn push(&mut self, mut level: usize, mut part: T) -> Result<(), StreamError> {
        loop {
            if level > usize::from(MAX_HEIGHT) {
                return Err(StreamError::Capacity);
            }
            if self.levels.len() == level {
                self.levels.push(Vec::new());
            }
            let parts = &mut self.levels[level];
            if parts.is_empty() {
                parts
                    .try_reserve_exact(T::FANOUT)
                    .map_err(|_| StreamError::Capacity)?;
            }
            parts.push(part);
            if parts.len() < T::FANOUT {
                return Ok(());
            }
            part = self.node(&self.levels[level]).await?;
            self.levels[level].clear();
            level += 1;
        }
    }

    async fn finish_tree(mut self) -> Result<(T, u8), StreamError> {
        for level in 0..=usize::from(MAX_HEIGHT) {
            let Some(parts) = self.levels.get(level) else {
                return Err(StreamError::InvalidArgument);
            };
            if parts.is_empty() {
                continue;
            }
            if level + 1 == self.levels.len() && parts.len() == 1 {
                return Ok((parts[0], level as u8));
            }
            let part = self.node(parts).await?;
            self.levels[level].clear();
            self.push(level + 1, part).await?;
        }
        Err(StreamError::Capacity)
    }
}

impl<S: Storage> Builder<'_, S> {
    async fn finish(self) -> Result<Root, StreamError> {
        let (part, height) = self.finish_tree().await?;
        Ok(Root { part, height })
    }
}

enum Update<'a> {
    Bytes(&'a [u8]),
    Reference(Root),
}

/// Incremental content writer. Failed or cancelled writes cannot be sealed as
/// successful prefixes; their pages remain owned by the enclosing stage.
pub struct Writer<'a, S> {
    builder: Builder<'a, S>,
    dependencies: Builder<'a, S, Edge>,
    buffer: Vec<u8>,
    total: u64,
    interrupted: bool,
}

impl<'a, S: Storage> Writer<'a, S> {
    /// Opens one private content tree without assigning any Stream position.
    pub fn new(storage: &'a S) -> Result<Self, StreamError> {
        let mut buffer = Vec::new();
        buffer
            .try_reserve_exact(PAGE_BYTES)
            .map_err(|_| StreamError::Capacity)?;
        buffer.extend_from_slice(BLOB_MAGIC);
        Ok(Self {
            builder: Builder {
                storage,
                levels: Vec::new(),
            },
            dependencies: Builder {
                storage,
                levels: Vec::new(),
            },
            buffer,
            total: 0,
            interrupted: false,
        })
    }

    /// Consumes bytes into canonical pages. Dropping this future during IO
    /// permanently retires the writer's ability to finish, without polling.
    pub async fn write(&mut self, bytes: &[u8]) -> Result<(), StreamError> {
        self.update(Update::Bytes(bytes)).await
    }

    /// Declares content reachable through this payload without appending bytes.
    /// The stage owner retains the supplied root until sealing or retirement.
    pub async fn reference(&mut self, root: Root) -> Result<(), StreamError> {
        self.update(Update::Reference(root)).await
    }

    async fn update(&mut self, update: Update<'_>) -> Result<(), StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        self.interrupted = true;
        let result = match update {
            Update::Bytes(bytes) => self.write_inner(bytes).await,
            Update::Reference(root) if root.height <= MAX_HEIGHT => {
                self.dependencies.push(0, Edge(root.part.digest)).await
            }
            Update::Reference(_) => Err(StreamError::InvalidArgument),
        };
        if result.is_ok() {
            self.interrupted = false;
        }
        result
    }

    async fn write_inner(&mut self, mut bytes: &[u8]) -> Result<(), StreamError> {
        self.total = self
            .total
            .checked_add(u64::try_from(bytes.len()).map_err(|_| StreamError::Capacity)?)
            .ok_or(StreamError::Capacity)?;
        while !bytes.is_empty() {
            let count = bytes.len().min(PAGE_BYTES - self.buffer.len());
            self.buffer.extend_from_slice(&bytes[..count]);
            bytes = &bytes[count..];
            if self.buffer.len() == PAGE_BYTES {
                let part = put(
                    self.builder.storage,
                    Kind::Blob,
                    std::mem::take(&mut self.buffer),
                    LEAF_BYTES as u64,
                )
                .await?;
                self.builder.push(0, part).await?;
                // Return to the shared native/browser scheduler once per physical page.
                crate::runtime::yield_now().await?;
                self.buffer
                    .try_reserve_exact(PAGE_BYTES)
                    .map_err(|_| StreamError::Capacity)?;
                self.buffer.extend_from_slice(BLOB_MAGIC);
            }
        }
        Ok(())
    }

    /// Seals the complete tree. An interrupted or failed writer has no root.
    pub async fn finish(mut self) -> Result<Root, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        if self.buffer.len() > BLOB_MAGIC.len() || self.total == 0 {
            let length = (self.buffer.len() - BLOB_MAGIC.len()) as u64;
            let part = put(self.builder.storage, Kind::Blob, self.buffer, length).await?;
            self.builder.push(0, part).await?;
        }
        let storage = self.builder.storage;
        let body = self.builder.finish().await?;
        if self.dependencies.levels.is_empty() {
            return Ok(body);
        }
        let height = body
            .height
            .checked_add(1)
            .filter(|height| *height <= MAX_HEIGHT)
            .ok_or(StreamError::Capacity)?;
        let (edge, _) = self.dependencies.finish_tree().await?;
        let bytes = encode_manifest([body.part], [edge.0])?;
        let part = put(storage, Kind::Manifest, bytes, body.part.length).await?;
        Ok(Root { part, height })
    }
}

/// Stages a stream through the one incremental content writer, independent of
/// input fragments. No page is visible as a record before stage publication.
pub async fn upload(
    storage: &impl Storage,
    input: impl Stream<Item = Result<OwnedBytes, StreamError>>,
) -> Result<Root, StreamError> {
    futures::pin_mut!(input);
    let mut writer = Writer::new(storage)?;
    while let Some(fragment) = input.next().await {
        let fragment = fragment?;
        writer.write(fragment.as_slice()).await?;
    }
    writer.finish().await
}
/// Lazy verified reader. The pending stack contains only bounded manifest pages
/// along one path, never an index or buffer proportional to total content size.
pub struct Reader<'a, S> {
    storage: &'a S,
    pending: Vec<Root>,
    skip: u64,
    remaining: u64,
    empty: bool,
    interrupted: bool,
}

impl<'a, S: Storage> Reader<'a, S> {
    /// Pins a root whose pages the enclosing read/stage owner keeps alive.
    pub fn new(storage: &'a S, root: Root) -> Result<Self, StreamError> {
        Self::open(storage, root, 0, root.part.length, true)
    }

    /// Pins a byte interval without reading its prefix or unselected subtrees.
    /// Empty intervals perform no IO. Each returned slice retains its original
    /// page owner; the enclosing read/stage keeps the root alive.
    pub fn range(
        storage: &'a S,
        root: Root,
        offset: u64,
        length: u64,
    ) -> Result<Self, StreamError> {
        Self::open(storage, root, offset, length, false)
    }

    fn open(
        storage: &'a S,
        root: Root,
        offset: u64,
        length: u64,
        whole: bool,
    ) -> Result<Self, StreamError> {
        if root.height > MAX_HEIGHT {
            return Err(StreamError::InvalidResponse);
        }
        if offset
            .checked_add(length)
            .is_none_or(|end| end > root.part.length)
        {
            return Err(StreamError::InvalidArgument);
        }
        Ok(Self {
            storage,
            pending: vec![root],
            skip: offset,
            remaining: length,
            empty: whole && root.part.length == 0,
            interrupted: false,
        })
    }

    /// Returns the next leaf with its original custody, verifying each page
    /// before interpretation. No full-record allocation or speculative IO.
    pub async fn next(&mut self) -> Result<Option<OwnedBytes>, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        self.interrupted = true;
        let result = self.next_page().await;
        if result.is_ok() {
            self.interrupted = false;
        } else {
            self.pending.clear();
        }
        result
    }
    async fn next_page(&mut self) -> Result<Option<OwnedBytes>, StreamError> {
        if self.remaining == 0 && !self.empty {
            self.pending.clear();
            return Ok(None);
        }
        while let Some(root) = self.pending.pop() {
            if root.part.length != 0 && self.skip >= root.part.length {
                self.skip -= root.part.length;
                continue;
            }
            let page = self.storage.get(root.part.digest).await?;
            if page.bytes.len() > PAGE_BYTES {
                return Err(StreamError::InvalidResponse);
            }
            verify(&root.part.digest, page.bytes.as_slice())?;
            if root.height == 0 {
                if page.kind != Kind::Blob
                    || !page.bytes.as_slice().starts_with(BLOB_MAGIC)
                    || (page.bytes.len() - BLOB_MAGIC.len()) as u64 != root.part.length
                {
                    return Err(StreamError::InvalidResponse);
                }
                let start = usize::try_from(self.skip).map_err(|_| StreamError::InvalidResponse)?;
                let length = usize::try_from(self.remaining.min(root.part.length - self.skip))
                    .map_err(|_| StreamError::InvalidResponse)?;
                self.skip = 0;
                self.remaining -= length as u64;
                self.empty = false;
                if self.remaining == 0 {
                    self.pending.clear();
                }
                crate::runtime::yield_now().await?;
                let start = BLOB_MAGIC.len() + start;
                return Ok(Some(page.bytes.slice(start..start + length)));
            }
            if page.kind != Kind::Manifest {
                return Err(StreamError::InvalidResponse);
            }
            let manifest = Manifest::decode(page.bytes.as_slice())
                .map_err(|_| StreamError::InvalidResponse)?;
            if manifest.parts().len() == 0
                || manifest.parts().len() > FANOUT
                || manifest.edges().len() > EDGE_FANOUT
                || manifest.parts().any(|part| part.length == 0)
                || manifest
                    .length()
                    .map_err(|_| StreamError::InvalidResponse)?
                    != root.part.length
            {
                return Err(StreamError::InvalidResponse);
            }
            self.pending
                .try_reserve(manifest.parts().len())
                .map_err(|_| StreamError::Capacity)?;
            // Reverse just this page so pop visits logical byte order.
            let start = self.pending.len();
            self.pending.extend(manifest.parts().map(|part| Root {
                part,
                height: root.height - 1,
            }));
            self.pending[start..].reverse();
        }
        Ok(None)
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
pub(crate) mod tests;
