/// Handle to one permanent Stream path.
pub struct Stream<P: ?Sized> {
    provider: Arc<P>,
    path: StreamPath,
}

impl<P: ?Sized> Clone for Stream<P> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            path: self.path.clone(),
        }
    }
}

impl<P: StreamProvider + ?Sized> Stream<P> {
    /// Binds a provider to one validated path.
    pub fn new(provider: Arc<P>, path: impl AsRef<str>) -> Result<Self, StreamError> {
        Ok(Self {
            provider,
            path: StreamPath::new(path)?,
        })
    }

    /// Permanent path.
    #[must_use]
    pub const fn path(&self) -> &StreamPath {
        &self.path
    }

    /// One atomic observation of history, durability and inherited origin.
    pub async fn head(&self) -> Result<PathHead, StreamError> {
        self.provider.head(self.path.clone()).await
    }

    /// Current tail.
    pub async fn tail(&self) -> Result<u64, StreamError> {
        self.provider.tail(self.path.clone()).await
    }

    /// Exact retained replay window observed atomically by the provider.
    pub async fn bounds(&self) -> Result<StreamBounds, StreamError> {
        self.provider.bounds(self.path.clone()).await
    }

    /// Unconditionally appends one record.
    pub async fn append(
        &self,
        value: impl Into<AppendRecord>,
        idempotency_key: IdempotencyKey,
    ) -> Result<AppendOutcome, StreamError> {
        self.append_batch(vec![value.into()], None, idempotency_key)
            .await
    }

    /// Appends one record only at the exact tail.
    pub async fn append_at(
        &self,
        value: impl Into<AppendRecord>,
        if_tail: u64,
        idempotency_key: IdempotencyKey,
    ) -> Result<AppendOutcome, StreamError> {
        self.append_batch(vec![value.into()], Some(if_tail), idempotency_key)
            .await
    }

    /// Atomically appends a contiguous batch.
    pub async fn append_batch(
        &self,
        records: Vec<AppendRecord>,
        if_tail: Option<u64>,
        idempotency_key: IdempotencyKey,
    ) -> Result<AppendOutcome, StreamError> {
        self.provider
            .append(AppendRequest {
                path: self.path.clone(),
                records,
                if_tail,
                idempotency_key: Some(idempotency_key),
            })
            .await
    }

    /// Forks the current or selected retained prefix.
    pub async fn fork(
        &self,
        destination: impl AsRef<str>,
        at_tail: Option<u64>,
        exclude: Vec<RelativePath>,
        idempotency_key: IdempotencyKey,
    ) -> Result<ForkReceipt, StreamError> {
        self.provider
            .fork(ForkRequest {
                exclude,
                source: self.path.clone(),
                destination: StreamPath::new(destination)?,
                at_tail,
                idempotency_key: Some(idempotency_key),
            })
            .await
    }

    /// Reads at most `limit` records from `from`.
    pub async fn read(&self, from: u64, limit: u32) -> Result<RecordStream, StreamError> {
        self.provider
            .read(ReadRequest {
                path: self.path.clone(),
                from,
                limit,
            })
            .await
    }

    /// Pages every record from `from` to the tail. See [`Replay`].
    #[must_use]
    pub fn replay(&self, from: u64) -> Replay<P> {
        Replay {
            stream: self.clone(),
            next: from,
            done: false,
        }
    }

    /// Replays from `from`, then remains live.
    pub async fn follow(
        &self,
        from: u64,
        filter: crate::FollowFilter,
    ) -> Result<impl futures::Stream<Item = Result<Record, StreamError>> + Unpin + ProviderTask + use<P>, StreamError> {
        Ok(records_only(self.provider.follow(FollowRequest {
            path: self.path.clone(),
            from,
            preceding: None,
            filter,
            progress: false,
        }).await?))
    }
}

/// Gapless, paged replay of one stream up to its tail.
pub struct Replay<P: ?Sized> {
    stream: Stream<P>,
    next: u64,
    done: bool,
}

impl<P: StreamProvider + ?Sized> Replay<P> {
    /// The next page in order, or `None` once the tail is reached.
    pub async fn next_page(&mut self) -> Result<Option<Vec<Record>>, StreamError> {
        use futures::TryStreamExt as _;
        if self.done {
            return Ok(None);
        }
        let page = match self.stream.read(self.next, REPLAY_PAGE).await {
            Ok(records) => records.try_collect::<Vec<_>>().await?,
            Err(StreamError::NotFound) if self.next == 0 => Vec::new(),
            Err(error) => return Err(error),
        };
        for record in &page {
            if record.sequence != self.next {
                return Err(StreamError::Unavailable);
            }
            self.next = self.next.checked_add(1).ok_or(StreamError::LimitExceeded)?;
        }
        self.done = page.is_empty();
        Ok((!self.done).then_some(page))
    }

    /// The sequence after the last record returned.
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.next
    }
}

/// Transport callers keep this allocation instead of copying a domain wrapper.
#[cfg(any(
    all(feature = "session", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
fn new_identity_bytes() -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(&new_identity())
}

/// Fresh provider identities share the same seeded entropy boundary.
fn new_identity() -> [u8; 16] {
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    {
        let mut bytes = [0; 16];
        if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
            return bytes;
        }
    }
    uuid::Uuid::new_v4().into_bytes()
}
