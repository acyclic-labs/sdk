use super::directory::{ROOT_BYTES, decode_root, location, root_bytes, row};
use super::*;
use crate::content::index::{Index, Sorter};

pub(super) const MAGIC: &[u8] = b"acyclic-stream-effects-v1\0";
pub(super) const HEADER: usize = MAGIC.len() + 9;
pub(super) const BYTES: usize = 1 + 8 + 8 + ROOT_BYTES;

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
}

pub(super) struct Writer<'a, S> {
    storage: &'a S,
    output: tree::Writer<'a, S>,
    empty: Option<Root>,
    count: u64,
}

impl<'a, S: Storage> Writer<'a, S> {
    pub(super) async fn new(storage: &'a S, deadline: Option<u64>) -> Result<Self, StreamError> {
        let mut output = tree::Writer::new(storage)?;
        output.write(MAGIC).await?;
        let mut bytes = [0; 9];
        bytes[0] = u8::from(deadline.is_some());
        bytes[1..].copy_from_slice(&deadline.unwrap_or(0).to_le_bytes());
        output.write(&bytes).await?;
        Ok(Self {
            storage,
            output,
            empty: None,
            count: 0,
        })
    }
    pub(super) async fn append(&mut self, pending: Pending<'a, S>) -> Result<(), StreamError> {
        if pending.ordinal != self.count {
            return Err(StreamError::InvalidArgument);
        }
        let conditions = pending.conditions.finish().await?;
        let checkpoint = match (conditions.is_empty(), self.empty) {
            (true, Some(root)) => root,
            (empty, _) => {
                let checkpoint = conditions.checkpoint(self.storage).await?;
                self.output.reference(checkpoint).await?;
                if empty {
                    self.empty = Some(checkpoint);
                }
                checkpoint
            }
        };
        let mut bytes = [0; BYTES];
        bytes[0] = u8::from(pending.soft);
        bytes[1..9].copy_from_slice(&pending.count.to_le_bytes());
        bytes[9..17].copy_from_slice(&pending.bookmark.effect_position()?.to_le_bytes());
        bytes[17..].copy_from_slice(&root_bytes(checkpoint));
        self.output.write(&bytes).await?;
        self.count = self.count.checked_add(1).ok_or(StreamError::Capacity)?;
        Ok(())
    }
    pub(super) async fn finish(self) -> Result<Root, StreamError> {
        self.output.finish().await
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
    root: Root,
    cursor: crate::content::tree::Cursor<'a, S>,
    remaining: u64,
    next: u64,
    deadline: Option<u64>,
    interrupted: bool,
}

impl<'a, S: Storage> Effects<'a, S> {
    pub(super) async fn open(
        storage: &'a S,
        staged: &StagedWrite,
        root: Root,
    ) -> Result<Self, StreamError> {
        let length = root
            .part
            .length
            .checked_sub(HEADER as u64)
            .ok_or(StreamError::InvalidResponse)?;
        if length % BYTES as u64 != 0 {
            return Err(StreamError::InvalidResponse);
        }
        if length / BYTES as u64 != staged.effects {
            return Err(StreamError::InvalidResponse);
        }
        let mut cursor = crate::content::tree::Cursor::new(storage, root)?;
        let header = cursor.exact::<{ MAGIC.len() }>().await?;
        crate::persistence::header(&header, b"acyclic-stream-effects-v", b"1\0")?;
        let bytes = cursor.exact::<9>().await?;
        let deadline = u64::from_le_bytes(
            bytes[1..]
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
        );
        if bytes[0] > 1 || (bytes[0] == 0 && deadline != 0) {
            return Err(StreamError::InvalidResponse);
        }
        let deadline = (bytes[0] == 1).then_some(deadline);
        if deadline != staged.deadline {
            return Err(StreamError::InvalidResponse);
        }
        Ok(Self {
            storage,
            staged: staged.clone(),
            root,
            cursor,
            remaining: length / BYTES as u64,
            next: 0,
            deadline,
            interrupted: false,
        })
    }

    /// Immutable metadata root pinned by the enclosing directory owner.
    pub const fn root(&self) -> Root {
        self.root
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
        if self.remaining == 0 {
            self.cursor.end().await?;
            return Ok(None);
        }
        let bytes = self.cursor.exact::<BYTES>().await?;
        if bytes[0] > 1 {
            return Err(StreamError::InvalidResponse);
        }
        let count = u64::from_le_bytes(
            bytes[1..9]
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
        );
        let position = u64::from_le_bytes(
            bytes[9..17]
                .try_into()
                .map_err(|_| StreamError::InvalidResponse)?,
        );
        let bookmark = Bookmark::at_effect(&self.staged, position, self.deadline, self.remaining)?;
        let checkpoint = decode_root(&bytes[17..])?;
        let conditions = if count == 0 {
            Index::default()
        } else {
            Index::open(self.storage, checkpoint).await?
        };
        if conditions.len() != count {
            return Err(StreamError::InvalidResponse);
        }
        let ordinal = self.next;
        self.next = self.next.checked_add(1).ok_or(StreamError::Capacity)?;
        self.remaining -= 1;
        Ok(Some(Effect {
            ordinal,
            soft: bytes[0] == 1,
            staged: self.staged.clone(),
            bookmark,
            conditions,
            checkpoint,
        }))
    }
}
