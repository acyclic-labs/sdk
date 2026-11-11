use super::directory::{ROOT_BYTES, body, decode_root, encode_root, location, row};
use super::*;
use crate::content::index::{Builder, Index, Sorter};

const MAGIC: &[u8] = b"acyclic-stream-effect-v1\0";
const BYTES: usize = MAGIC.len() + 8 + 1 + super::decode::BOOKMARK_BYTES + ROOT_BYTES;

pub(super) struct Pending<'a, S> {
    pub(super) ordinal: u64,
    pub(super) mutation: u64,
    soft: bool,
    bookmark: Bookmark,
    conditions: Sorter<'a, S>,
    count: u64,
}

impl<'a, S: Storage> Pending<'a, S> {
    pub(super) fn new(storage: &'a S, ordinal: u64, soft: bool, bookmark: Bookmark) -> Self {
        Self {
            ordinal,
            mutation: 0,
            soft,
            bookmark,
            conditions: Sorter::new(storage),
            count: 0,
        }
    }
    pub(super) async fn condition(
        &mut self,
        path: Field,
        bookmark: &Bookmark,
    ) -> Result<(), StreamError> {
        self.conditions
            .push(path, row(self.ordinal, self.count, bookmark))
            .await?;
        self.count = self.count.checked_add(1).ok_or(StreamError::Capacity)?;
        Ok(())
    }
    pub(super) async fn finish(
        self,
        storage: &'a S,
        effects: &mut Builder<'a, S>,
    ) -> Result<(), StreamError> {
        let conditions = self.conditions.finish().await?;
        let checkpoint = conditions.checkpoint(storage).await?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(BYTES)
            .map_err(|_| StreamError::Capacity)?;
        bytes.extend_from_slice(MAGIC);
        bytes.extend_from_slice(&self.ordinal.to_le_bytes());
        bytes.push(u8::from(self.soft));
        bytes.extend_from_slice(&self.bookmark.encode());
        encode_root(&mut bytes, checkpoint);
        let mut writer = tree::Writer::new(storage)?;
        writer.reference(checkpoint).await?;
        writer.write(&bytes).await?;
        let root = writer.finish().await?;
        effects
            .push(
                Field::Inline(OwnedBytes::copy_from_slice(&self.ordinal.to_be_bytes())),
                Field::Content(root),
            )
            .await
    }
}

/// Original effect metadata with a private, unevaluated condition index.
pub struct Effect {
    /// Zero-based caller ordinal, independent of path ordering.
    pub ordinal: u64,
    /// Whether this effect is skipped when its final-apply condition fails.
    pub soft: bool,
    staged: StagedWrite,
    bookmark: Bookmark,
    conditions: Index,
    checkpoint: Root,
}

impl Effect {
    /// Private condition checkpoint retained by the enclosing directory graph.
    pub const fn condition_root(&self) -> Root {
        self.checkpoint
    }

    /// Opens conditions in byte-path order, without evaluating them.
    pub async fn conditions<'a, S: Storage>(
        &self,
        storage: &'a S,
    ) -> Result<Cursor<'a, S>, StreamError> {
        Cursor::open(storage, &self.staged, &self.conditions).await
    }

    /// Looks up one original condition boundary by logical path bytes.
    pub async fn condition(
        &self,
        storage: &impl Storage,
        path: Field,
    ) -> Result<Option<Location>, StreamError> {
        let Some(value) = self.conditions.get(storage, path).await? else {
            return Ok(None);
        };
        Ok(Some(location(&self.staged, value)?))
    }

    /// Resumes the original effect header without rescanning previous effects.
    pub fn reader<'a, S: Storage>(
        &self,
        storage: &'a S,
        staged: &StagedWrite,
    ) -> Result<Reader<'a, S>, StreamError> {
        Reader::resume(storage, staged, &self.bookmark)
    }
}

/// Lazy original effects from one pinned private directory.
pub struct Effects<'a, S> {
    storage: &'a S,
    staged: StagedWrite,
    cursor: crate::content::index::Cursor<'a, S>,
    interrupted: bool,
}

impl<'a, S: Storage> Effects<'a, S> {
    pub(super) async fn open(
        storage: &'a S,
        staged: &StagedWrite,
        index: &Index,
    ) -> Result<Self, StreamError> {
        Ok(Self {
            storage,
            staged: staged.clone(),
            cursor: index.scan(storage, None).await?,
            interrupted: false,
        })
    }

    /// Returns one original effect; cancellation cannot skip consumed metadata.
    pub async fn next(&mut self) -> Result<Option<Effect>, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        self.interrupted = true;
        let result = self.next_inner().await;
        if result.is_ok() {
            self.interrupted = false;
        }
        result
    }

    async fn next_inner(&mut self) -> Result<Option<Effect>, StreamError> {
        let Some(entry) = self.cursor.next().await? else {
            return Ok(None);
        };
        let (Field::Inline(key), Field::Content(root)) = entry else {
            return Err(StreamError::InvalidResponse);
        };
        let ordinal = u64::from_be_bytes(
            key.as_slice()
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
        );
        let bytes = body(self.storage, root, BYTES).await?;
        let bytes =
            crate::persistence::header(bytes.as_slice(), b"acyclic-stream-effect-v", b"1\0")?;
        if bytes.len() != BYTES - MAGIC.len() || bytes[8] > 1 {
            return Err(StreamError::InvalidResponse);
        }
        let stored = u64::from_le_bytes(
            bytes[..8]
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
        );
        if stored != ordinal {
            return Err(StreamError::InvalidResponse);
        }
        let end = 9 + super::decode::BOOKMARK_BYTES;
        let bookmark = Bookmark::decode(&self.staged, &bytes[9..end])?;
        let checkpoint = decode_root(&bytes[end..])?;
        let conditions = Index::open(self.storage, checkpoint).await?;
        Ok(Some(Effect {
            ordinal,
            soft: bytes[8] == 1,
            staged: self.staged.clone(),
            bookmark,
            conditions,
            checkpoint,
        }))
    }
}
