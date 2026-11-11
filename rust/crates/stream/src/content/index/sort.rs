use super::*;
use std::cmp::Ordering;

/// Orders arbitrarily many byte-key descriptors with bounded working pages.
/// Keys are unique by logical bytes, independent of content representation.
pub struct Sorter<'a, S> {
    storage: &'a S,
    buffer: Vec<Entry>,
    bytes: usize,
    runs: Vec<Option<Index>>,
    interrupted: bool,
}

impl<'a, S: Storage> Sorter<'a, S> {
    /// Opens private external ordering in the enclosing retained stage.
    pub fn new(storage: &'a S) -> Self {
        Self {
            storage,
            buffer: Vec::new(),
            bytes: 0,
            runs: Vec::new(),
            interrupted: false,
        }
    }
    /// Adds one descriptor. Input order does not allocate a whole-request sort.
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
        let key = Key::stored(self.storage, key).await?;
        let ordered_tail = match self.buffer.last() {
            Some(last) => last.key.compare(self.storage, &key).await? == Ordering::Less,
            None => true,
        };
        let position = if ordered_tail {
            self.buffer.len()
        } else {
            match search(self.storage, &self.buffer, &key, |entry| &entry.key).await? {
                Ok(_) => return Err(StreamError::InvalidArgument),
                Err(position) => position,
            }
        };
        let value = key::store(self.storage, value, INLINE_FIELD_BYTES).await?;
        let entry = Entry { key, value };
        self.buffer
            .try_reserve(1)
            .map_err(|_| StreamError::Capacity)?;
        self.bytes += codec::entry_length(&entry);
        self.buffer.insert(position, entry);
        if self.bytes >= tree::LEAF_BYTES / 2 {
            self.flush().await?;
        }
        Ok(())
    }
    async fn flush(&mut self) -> Result<(), StreamError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut builder = Builder::new(self.storage);
        for entry in std::mem::take(&mut self.buffer) {
            builder.append(entry).await?;
        }
        self.bytes = 0;
        let mut run = builder.finish().await?;
        for level in 0..u64::BITS as usize {
            if self.runs.len() == level {
                self.runs
                    .try_reserve(1)
                    .map_err(|_| StreamError::Capacity)?;
                self.runs.push(None);
            }
            if let Some(previous) = self.runs[level].take() {
                run = merge(self.storage, previous, run).await?;
            } else {
                self.runs[level] = Some(run);
                return Ok(());
            }
        }
        Err(StreamError::Capacity)
    }
    /// Seals all ordered entries. A duplicate, failed or cancelled input cannot
    /// return a successful prefix; its pages remain owned by the stage.
    pub async fn finish(mut self) -> Result<Index, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        self.flush().await?;
        let mut output = None;
        for run in self.runs.into_iter().flatten() {
            output = Some(match output {
                Some(previous) => merge(self.storage, previous, run).await?,
                None => run,
            });
        }
        Ok(output.unwrap_or_default())
    }
}

async fn merge(storage: &impl Storage, left: Index, right: Index) -> Result<Index, StreamError> {
    let mut left = Cursor::open(storage, &left, None, true).await?;
    let mut right = Cursor::open(storage, &right, None, true).await?;
    let mut a = left.next_entry().await?;
    let mut b = right.next_entry().await?;
    let mut output = Builder::new(storage);
    loop {
        let choose_left = match (&a, &b) {
            (Some(a), Some(b)) => {
                let ordering = a.key.compare(storage, &b.key).await?;
                if ordering == Ordering::Equal {
                    return Err(StreamError::InvalidArgument);
                }
                ordering == Ordering::Less
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => return output.finish().await,
        };
        if choose_left {
            output
                .append(a.take().ok_or(StreamError::InvalidResponse)?)
                .await?;
            a = left.next_entry().await?;
        } else {
            output
                .append(b.take().ok_or(StreamError::InvalidResponse)?)
                .await?;
            b = right.next_entry().await?;
        }
    }
}
