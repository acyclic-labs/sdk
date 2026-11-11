use super::*;
use std::cmp::Ordering;

struct Level {
    page: Page<Format>,
    bytes: usize,
}
impl Level {
    fn new(height: usize) -> Self {
        Self {
            page: if height == 0 {
                Page::Leaf(Vec::new())
            } else {
                Page::Internal(Vec::new())
            },
            bytes: 0,
        }
    }
    fn len(&self) -> usize {
        match &self.page {
            Page::Leaf(entries) => entries.len(),
            Page::Internal(children) => children.len(),
        }
    }
    fn chunk(&self, height: usize) -> Result<Option<(usize, bool)>, StreamError> {
        fn chunk<T>(
            items: &[T],
            height: usize,
            size: impl Fn(&T) -> usize,
            hash: impl Fn(&T) -> u64,
        ) -> Result<Option<(usize, bool)>, StreamError> {
            let limits = page::PackingLimits {
                maximum_items: tree::LEAF_BYTES,
                maximum_bytes: tree::LEAF_BYTES,
                maximum_object_bytes: tree::LEAF_BYTES,
            };
            let mut chunks = page::PageChunks::<_, _, _, HEADER, TARGET>::new(
                items,
                limits,
                height as u16,
                |item| Ok(size(item)),
                hash,
            )
            .map_err(packing)?;
            Ok(chunks
                .next_page(|| Ok(()))
                .map_err(packing)?
                .map(|(items, _, closed)| (items.len(), closed)))
        }
        match &self.page {
            Page::Leaf(entries) => chunk(entries, height, codec::entry_length, |entry| {
                entry.key.hash()
            }),
            Page::Internal(children) => chunk(children, height, codec::child_length, |child| {
                child.first.hash()
            }),
        }
    }
    fn take(&mut self, count: usize) -> Result<Page<Format>, StreamError> {
        fn take<T>(entries: &mut Vec<T>, count: usize) -> Result<Vec<T>, StreamError> {
            if count > entries.len() {
                return Err(StreamError::InvalidArgument);
            }
            let mut output = Vec::new();
            output
                .try_reserve_exact(count)
                .map_err(|_| StreamError::Capacity)?;
            output.extend(entries.drain(..count));
            Ok(output)
        }
        let result = match &mut self.page {
            Page::Leaf(entries) => {
                let entries = take(entries, count)?;
                self.bytes -= entries.iter().map(codec::entry_length).sum::<usize>();
                Page::Leaf(entries)
            }
            Page::Internal(children) => {
                let children = take(children, count)?;
                self.bytes -= children.iter().map(codec::child_length).sum::<usize>();
                Page::Internal(children)
            }
        };
        Ok(result)
    }
}
fn packing(error: page::PackingError<StreamError>) -> StreamError {
    match error {
        page::PackingError::Encode(error) => error,
        _ => StreamError::InvalidArgument,
    }
}
fn borrowed(page: &Page<Format>) -> PageRef<'_, Format> {
    match page {
        Page::Leaf(entries) => PageRef::Leaf(entries),
        Page::Internal(children) => PageRef::Internal(children),
    }
}

/// Builds one immutable index from strictly increasing byte keys.
/// Unfinished tails retain bounded pages at each fixed-width level. Cancellation
/// or failure retires this builder; it cannot seal an accepted prefix afterward.
pub struct Builder<'a, S> {
    storage: &'a S,
    levels: Vec<Level>,
    last: Option<Key>,
    count: u64,
    interrupted: bool,
}
impl<'a, S: Storage> Builder<'a, S> {
    /// Opens an unpublished ordered index in the enclosing retained stage.
    pub fn new(storage: &'a S) -> Self {
        Self {
            storage,
            levels: Vec::new(),
            last: None,
            count: 0,
            interrupted: false,
        }
    }
    fn level(&mut self, height: usize) -> Result<&mut Level, StreamError> {
        if height > usize::from(MAX_HEIGHT) {
            return Err(StreamError::Capacity);
        }
        while self.levels.len() <= height {
            self.levels
                .try_reserve(1)
                .map_err(|_| StreamError::Capacity)?;
            self.levels.push(Level::new(self.levels.len()));
        }
        Ok(&mut self.levels[height])
    }
    fn child(&mut self, height: usize, child: Child<Key, Root>) -> Result<(), StreamError> {
        let level = self.level(height)?;
        let Page::Internal(children) = &mut level.page else {
            return Err(StreamError::InvalidArgument);
        };
        children.try_reserve(1).map_err(|_| StreamError::Capacity)?;
        level.bytes += codec::child_length(&child);
        children.push(child);
        Ok(())
    }
    async fn flush(&mut self, start: usize, close: bool) -> Result<(), StreamError> {
        let mut pending = Vec::new();
        pending
            .try_reserve_exact(usize::from(MAX_HEIGHT) + 1)
            .map_err(|_| StreamError::Capacity)?;
        let mut height = start;
        loop {
            let level = &mut self.levels[height];
            let force = close && height == start;
            let chunk = if force || level.bytes >= tree::LEAF_BYTES / 2 {
                level.chunk(height)?
            } else {
                None
            };
            let Some((count, _)) = chunk.filter(|(_, closed)| *closed || force) else {
                let Some(previous) = pending.pop() else {
                    return Ok(());
                };
                height = previous;
                continue;
            };
            let page = level.take(count)?;
            let child = codec::write(self.storage, borrowed(&page), height as u8).await?;
            self.child(height + 1, child)?;
            pending.push(height);
            height += 1;
        }
    }
    /// Adds one complete logical key/value, promoting large fields to content.
    /// Equal or decreasing keys refuse before any index root is returned.
    pub async fn push(&mut self, key: Field, value: Field) -> Result<(), StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        self.interrupted = true;
        let result = self.push_inner(key, value).await;
        if result.is_ok() {
            self.interrupted = false;
        }
        result
    }
    async fn push_inner(&mut self, key: Field, value: Field) -> Result<(), StreamError> {
        let field = key::store(self.storage, key, key::INLINE_KEY).await?;
        let key = Key::new(self.storage, field).await?;
        if let Some(last) = &self.last
            && last.compare(self.storage, &key).await? != Ordering::Less
        {
            return Err(StreamError::InvalidArgument);
        }
        let value = key::store(self.storage, value, INLINE_FIELD_BYTES).await?;
        let count = self.count.checked_add(1).ok_or(StreamError::Capacity)?;
        let last = key.clone();
        let level = self.level(0)?;
        let Page::Leaf(entries) = &mut level.page else {
            return Err(StreamError::InvalidArgument);
        };
        let entry = Entry { key, value };
        entries.try_reserve(1).map_err(|_| StreamError::Capacity)?;
        level.bytes += codec::entry_length(&entry);
        entries.push(entry);
        self.flush(0, false).await?;
        self.count = count;
        self.last = Some(last);
        Ok(())
    }
    /// Seals the whole index, never a prefix of failed or interrupted input.
    pub async fn finish(mut self) -> Result<Index, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        if self.count == 0 {
            return Ok(Index::default());
        }
        for height in 0..=usize::from(MAX_HEIGHT) {
            if self.levels.get(height).is_none_or(|level| level.len() == 0) {
                continue;
            }
            if self
                .levels
                .iter()
                .skip(height + 1)
                .all(|level| level.len() == 0)
            {
                if let Page::Internal(children) = &self.levels[height].page
                    && children.len() == 1
                {
                    return Ok(Index {
                        root: Some(children[0].page),
                        height: height as u8 - 1,
                        count: self.count,
                    });
                }
                let count = self.levels[height].len();
                let page = self.levels[height].take(count)?;
                let child = codec::write(self.storage, borrowed(&page), height as u8).await?;
                return Ok(Index {
                    root: Some(child.page),
                    height: height as u8,
                    count: self.count,
                });
            }
            self.flush(height, true).await?;
        }
        Err(StreamError::Capacity)
    }
}
