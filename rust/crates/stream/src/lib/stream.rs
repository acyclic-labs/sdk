/// Handle to one permanent Stream path.
pub struct Stream<P> {
    provider: Arc<P>,
    path: StreamPath,
}

impl<P> Clone for Stream<P> {
    fn clone(&self) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            path: self.path.clone(),
        }
    }
}

impl<P: StreamProvider> Stream<P> {
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

    /// Current tail.
    pub async fn tail(&self) -> Result<u64, StreamError> {
        self.provider.tail(self.path.clone()).await
    }

    /// Exact retained replay window observed atomically by the provider.
    pub async fn bounds(&self) -> Result<StreamBounds, StreamError> {
        self.provider.bounds(self.path.clone()).await
    }

    /// Unconditionally appends one record.
    pub async fn append(&self, value: impl Into<Bytes>) -> Result<AppendOutcome, StreamError> {
        self.append_batch(vec![value.into()], None, None).await
    }

    /// Appends one record only at the exact tail.
    pub async fn append_at(
        &self,
        value: impl Into<Bytes>,
        if_tail: u64,
    ) -> Result<AppendOutcome, StreamError> {
        self.append_batch(vec![value.into()], Some(if_tail), None)
            .await
    }

    /// Atomically appends a contiguous batch.
    pub async fn append_batch(
        &self,
        records: Vec<Bytes>,
        if_tail: Option<u64>,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<AppendOutcome, StreamError> {
        self.provider
            .append(AppendRequest {
                path: self.path.clone(),
                records,
                if_tail,
                idempotency_key: Some(idempotency_key.unwrap_or_else(new_idempotency_key)),
            })
            .await
    }

    /// Forks the current or selected retained prefix.
    pub async fn fork(
        &self,
        destination: impl AsRef<str>,
        at_tail: Option<u64>,
        idempotency_key: Option<IdempotencyKey>,
    ) -> Result<ForkReceipt, StreamError> {
        self.provider
            .fork(ForkRequest {
                source: self.path.clone(),
                destination: StreamPath::new(destination)?,
                at_tail,
                idempotency_key: Some(idempotency_key.unwrap_or_else(new_idempotency_key)),
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
    pub async fn follow(&self, from: u64) -> Result<RecordStream, StreamError> {
        self.provider.follow(self.path.clone(), from).await
    }
}

/// Gapless, paged replay of one stream up to its tail.
pub struct Replay<P> {
    stream: Stream<P>,
    next: u64,
    done: bool,
}

impl<P: StreamProvider> Replay<P> {
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

fn new_idempotency_key() -> IdempotencyKey {
    IdempotencyKey(Bytes::copy_from_slice(uuid::Uuid::new_v4().as_bytes()))
}
