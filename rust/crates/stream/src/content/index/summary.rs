//! Paged child summaries for unpublished rewrites, never authority roots.
use super::*;

const MAGIC: &[u8] = b"acyclic-index-summaries-v1\0";
const META: &[u8] = b"acyclic-index-summary-root-v1\0";
const META_BYTES: usize = META.len() + 1 + 8 + 41;
const MINIMUM_ROW: usize = 32 + 9 + 41;
const MAXIMUM_ROW: usize = 32 + 9 + key::INLINE_KEY + 41;

/// One immutable summary sequence, whose owner retains its complete graph.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Summaries {
    body: Root,
    count: u64,
    height: u8,
}

/// Bounded sequence construction for an original node level.
pub struct Writer<'a, S> {
    storage: &'a S,
    output: tree::Writer<'a, S>,
    last: Option<Key>,
    count: u64,
    height: u8,
    interrupted: bool,
}

impl<'a, S: Storage> Writer<'a, S> {
    /// Opens an unpublished sequence; successful puts stay on its stage owner.
    pub async fn new(storage: &'a S, height: u8) -> Result<Self, StreamError> {
        if height > MAX_HEIGHT {
            return Err(StreamError::InvalidArgument);
        }
        let mut output = tree::Writer::new(storage)?;
        output.write(MAGIC).await?;
        output.write(&[height]).await?;
        Ok(Self {
            storage,
            output,
            last: None,
            count: 0,
            height,
            interrupted: false,
        })
    }

    /// Appends a strictly increasing bound and complete page reference.
    /// Page identity/level come from the caller's validated original IO path.
    pub async fn push(&mut self, first: Field, page: Root) -> Result<(), StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        self.interrupted = true;
        let result = async {
            let first = Key::stored(self.storage, first).await?;
            if let Some(last) = &self.last
                && last.compare(self.storage, &first).await? != std::cmp::Ordering::Less
            {
                return Err(StreamError::InvalidArgument);
            }
            if page.height > MAX_HEIGHT
                || page.part.length < HEADER as u64
                || page.part.length > tree::LEAF_BYTES as u64
            {
                return Err(StreamError::InvalidArgument);
            }
            let mut bytes = Vec::new();
            bytes
                .try_reserve_exact(codec::child_length(&Child {
                    first: first.clone(),
                    page,
                }))
                .map_err(|_| StreamError::Capacity)?;
            bytes.extend_from_slice(&first.prefix);
            codec::field_bytes(&mut self.output, &mut bytes, &first.field).await?;
            self.output.reference(page).await?;
            codec::root_bytes(&mut bytes, page);
            self.output.write(&bytes).await?;
            self.count = self.count.checked_add(1).ok_or(StreamError::Capacity)?;
            self.last = Some(first);
            Ok(())
        }
        .await;
        if result.is_ok() {
            self.interrupted = false;
        }
        result
    }

    /// Seals the whole sequence, never a failed or cancelled prefix.
    pub async fn finish(self) -> Result<Summaries, StreamError> {
        if self.interrupted {
            return Err(StreamError::Unavailable);
        }
        Ok(Summaries {
            body: self.output.finish().await?,
            count: self.count,
            height: self.height,
        })
    }
}

impl Summaries {
    /// Complete row count bound to this immutable descriptor.
    pub const fn len(&self) -> u64 {
        self.count
    }
    /// Whether this sequence has no child rows.
    pub const fn is_empty(&self) -> bool {
        self.count == 0
    }
    /// Index-node level of every supplied child page.
    pub const fn data_height(&self) -> u8 {
        self.height
    }
    /// Content root retained by the enclosing stage or checkpoint.
    pub const fn body(&self) -> Root {
        self.body
    }

    /// Persists the small versioned descriptor and its complete reference graph.
    pub async fn checkpoint(&self, storage: &impl Storage) -> Result<Root, StreamError> {
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(META_BYTES)
            .map_err(|_| StreamError::Capacity)?;
        bytes.extend_from_slice(META);
        bytes.push(self.height);
        bytes.extend_from_slice(&self.count.to_le_bytes());
        codec::root_bytes(&mut bytes, self.body);
        let mut writer = tree::Writer::new(storage)?;
        writer.reference(self.body).await?;
        writer.write(&bytes).await?;
        writer.finish().await
    }

    /// Opens an authenticated descriptor, without materializing its rows.
    pub async fn open(storage: &impl Storage, checkpoint: Root) -> Result<Self, StreamError> {
        let bytes = codec::body(storage, checkpoint, META_BYTES, META_BYTES).await?;
        let mut decode = codec::Decode {
            bytes: crate::persistence::header(&bytes, b"acyclic-index-summary-root-v", b"1\0")?,
        };
        let height = decode.array::<1>()?[0];
        let count = u64::from_le_bytes(decode.array()?);
        let body = decode.root()?;
        if height > MAX_HEIGHT || !decode.bytes.is_empty() {
            return Err(StreamError::InvalidResponse);
        }
        let result = Self {
            body,
            count,
            height,
        };
        result.cursor(storage).await?;
        Ok(result)
    }

    /// Opens ordered rows with bounded working pages and no prefix scan.
    pub async fn cursor<'a, S: Storage>(
        &self,
        storage: &'a S,
    ) -> Result<Cursor<'a, S>, StreamError> {
        let bytes = self
            .body
            .part
            .length
            .checked_sub((MAGIC.len() + 1) as u64)
            .ok_or(StreamError::InvalidResponse)?;
        if self.height > MAX_HEIGHT
            || self.count > bytes / MINIMUM_ROW as u64
            || self.count < bytes.div_ceil(MAXIMUM_ROW as u64)
        {
            return Err(StreamError::InvalidResponse);
        }
        let mut input = tree::Cursor::new(storage, self.body)?;
        let magic = input.exact::<{ MAGIC.len() }>().await?;
        crate::persistence::header(&magic, b"acyclic-index-summaries-v", b"1\0")?;
        if input.byte().await? != self.height {
            return Err(StreamError::InvalidResponse);
        }
        Ok(Cursor {
            input,
            remaining: self.count,
            interrupted: false,
        })
    }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// One immutable summary descriptor, lazily decoded in caller order.
pub struct Cursor<'a, S> {
    input: tree::Cursor<'a, S>,
    remaining: u64,
    interrupted: bool,
}

impl<S: Storage> Cursor<'_, S> {
    /// Returns the next bound/reference; failure or cancellation retires this cursor.
    pub async fn next(&mut self) -> Result<Option<(Field, Root)>, StreamError> {
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
    async fn next_inner(&mut self) -> Result<Option<(Field, Root)>, StreamError> {
        if self.remaining == 0 {
            self.input.end().await?;
            return Ok(None);
        }
        let prefix = self.input.exact::<32>().await?;
        let key = Key::decoded(prefix, self.input.field().await?)?;
        let bytes = self.input.exact::<41>().await?;
        let page = codec::Decode { bytes: &bytes }.root()?;
        if page.part.length < HEADER as u64 || page.part.length > tree::LEAF_BYTES as u64 {
            return Err(StreamError::InvalidResponse);
        }
        self.remaining -= 1;
        Ok(Some((key.field, page)))
    }
}
