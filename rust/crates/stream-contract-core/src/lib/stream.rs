/// Handle to one permanent Stream path.
pub struct Stream<P: ?Sized> {
    provider: ProviderOwner<P>,
    path: StreamPath,
    fork_receipt: Option<ForkReceipt>,
}

impl<P: ?Sized> Clone for Stream<P> {
    fn clone(&self) -> Self {
        Self {
            provider: self.provider.clone(),
            path: self.path.clone(),
            fork_receipt: self.fork_receipt.clone(),
        }
    }
}

impl<P: StreamProvider + ?Sized> Stream<P> {
    /// Binds a provider to one validated path.
    pub fn new(provider: ProviderOwner<P>, path: impl AsRef<str>) -> Result<Self, StreamError> {
        Ok(Self {
            provider,
            path: StreamPath::new(path)?,
            fork_receipt: None,
        })
    }

    /// Permanent path.
    #[must_use]
    pub const fn path(&self) -> &StreamPath {
        &self.path
    }

    /// Original fork publication when this handle was returned by [`Self::fork`].
    /// Rebinding the same path with [`Self::new`] does not reconstruct a receipt.
    pub const fn fork_receipt(&self) -> Option<&ForkReceipt> {
        self.fork_receipt.as_ref()
    }

    /// Submits one canonical commit across this provider's paths. The returned
    /// owned handle outlives this Stream and any waiter; cancellation cannot
    /// withdraw a submitted publication. Its exact identity is recoverable.
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    pub fn submit(&self, request: CommitRequest) -> crate::Pending {
        crate::Pending::submit(
            self.provider.clone(),
            request,
            &crate::runtime::PlatformExecutor,
        )
    }

    /// Transfer one original append to the shared owned-publication mechanism.
    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
    pub fn submit_append(&self, request: AppendRequest) -> crate::AppendPending {
        crate::AppendPending::submit_append(
            self.provider.clone(),
            request,
            &crate::runtime::PlatformExecutor,
        )
    }

    /// One atomic observation of history, durability and inherited origin.
    pub async fn head(&self) -> Result<PathHead, StreamError> {
        self.provider.head(self.path.clone()).await
    }

    /// Current tail.
    pub async fn tail(&self) -> Result<u64, StreamError> {
        self.provider.tail(self.path.clone()).await
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
                idempotency_key,
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
    ) -> Result<Self, StreamError> {
        let receipt = self
            .provider
            .fork(ForkRequest {
                exclude,
                source: self.path.clone(),
                destination: StreamPath::new(destination)?,
                at_tail,
                idempotency_key,
            })
            .await?;
        Ok(Self {
            provider: self.provider.clone(),
            path: receipt.destination.clone(),
            fork_receipt: Some(receipt),
        })
    }

    /// Reads at most `limit` records from `from`.
    pub async fn read(&self, from: u64, limit: u32) -> Result<RecordStream, StreamError> {
        self.read_request(ReadRequest {
            key: None,
            path: self.path.clone(),
            from,
            limit,
            at_least: Default::default(),
        })
        .await
    }

    /// Reads matching opaque header keys in a bounded historical interval.
    pub async fn seek(
        &self,
        key: ReadKeySeek,
        from: u64,
        limit: u32,
    ) -> Result<RecordStream, StreamError> {
        self.read_request(ReadRequest {
            key: Some(key),
            path: self.path.clone(),
            from,
            limit,
            at_least: Default::default(),
        })
        .await
    }

    async fn read_request(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        let keyed = request.key.is_some();
        #[cfg(feature = "serde")]
        let diagnostic = crate::trace::is_source(&request.path);
        let work = self.provider.read(request);
        #[cfg(feature = "serde")]
        if diagnostic {
            return work.await;
        }
        crate::trace::observe(
            &crate::trace::Request {
                name: if keyed {
                    "stream.seek_open"
                } else {
                    "stream.read_open"
                },
                mechanism: crate::trace::Mechanism::Io,
                parent: None,
                guarantee: Some(if keyed {
                    "bounded_header_key_selection"
                } else {
                    "bounded_ordered_record_cursor"
                }),
                key: None,
            },
            work,
            |_| None,
        )
        .await
        .map(crate::trace::records)
    }
    /// The latest record carrying `key`, by one indexed lookup.
    pub async fn latest(&self, key: impl Into<Bytes>) -> Result<Option<Record>, StreamError> {
        self.provider
            .latest(self.path.clone(), key.into(), Default::default())
            .await
    }

    /// One page of this stream's direct children after `after`.
    pub async fn children(
        &self,
        after: Option<StreamPath>,
        limit: u32,
    ) -> Result<ChildrenPage, StreamError> {
        self.provider
            .children_page(ChildrenPageRequest {
                offset: 0,
                tail_stream: None,
                parent: Some(self.path.clone()),
                after,
                hierarchy_version: None,
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
            until: None,
            done: false,
        }
    }

    /// Replays from `from`, then remains live.
    pub async fn follow(
        &self,
        from: u64,
        filter: crate::FollowFilter,
    ) -> Result<
        impl futures::Stream<Item = Result<Record, StreamError>> + Unpin + ProviderTask + use<P>,
        StreamError,
    > {
        #[cfg(feature = "serde")]
        let diagnostic = crate::trace::is_source(&self.path);
        let work = async {
            self.provider
                .follow(FollowRequest {
                    bootstrap_head: false,
                    until: None,
                    when_absent: Default::default(),
                    path: self.path.clone(),
                    from,
                    preceding: None,
                    filter,
                    progress: false,
                    at_least: Default::default(),
                })
                .await
                .map(records_only)
        };
        #[cfg(feature = "serde")]
        if diagnostic {
            return work.await;
        }
        crate::trace::observe(
            &crate::trace::Request {
                name: "stream.follow_open",
                mechanism: crate::trace::Mechanism::Io,
                parent: None,
                guarantee: Some("ordered_history_and_live_cursor"),
                key: None,
            },
            work,
            |_| None,
        )
        .await
        .map(crate::trace::records)
    }
}

/// Gapless, paged replay of one stream up to its tail.
pub struct Replay<P: ?Sized> {
    stream: Stream<P>,
    next: u64,
    until: Option<u64>,
    done: bool,
}

#[derive(Debug, thiserror::Error)]
/// A replay page that broke the requested prefix.
pub enum ReplayViolation {
    #[error("replay page returned {actual} records beyond its requested bound {limit}")]
    /// A page returned more records than its requested bound.
    OversizedPage {
        /// The bound the page was asked for.
        limit: u32,
        /// The records the page returned.
        actual: usize,
    },
    #[error("replay expected record {expected}, received {actual}, exclusive end {end:?}")]
    /// A record arrived out of sequence.
    Discontinuity {
        /// The sequence the replay required next.
        expected: u64,
        /// The sequence the page returned.
        actual: u64,
        /// The exclusive end the replay requested.
        end: Option<u64>,
    },
    #[error("replay prefix ended at {cursor} before the requested exclusive end {end}")]
    /// The prefix ended before the requested exclusive end.
    IncompletePrefix {
        /// The sequence the prefix stopped at.
        cursor: u64,
        /// The exclusive end the replay requested.
        end: u64,
    },
    #[error("replay cursor cannot advance beyond the sequence representation")]
    /// The cursor cannot advance beyond the sequence representation.
    CursorExhausted,
}

impl<P: StreamProvider + ?Sized> Replay<P> {
    /// Stops before `sequence`, even if the stream grows while pages are read.
    /// An end at or before the cursor is empty; an incomplete prefix refuses.
    #[must_use]
    pub fn until(mut self, sequence: u64) -> Self {
        self.until = Some(sequence);
        self
    }

    /// The next page in order, or `None` once the tail is reached.
    pub async fn next_page(&mut self) -> Result<Option<Vec<Record>>, StreamError> {
        let (page, next, done) = self.read_page().await?;
        self.next = next;
        self.done = done;
        Ok(page)
    }

    async fn read_page(&self) -> Result<(Option<Vec<Record>>, u64, bool), StreamError> {
        use futures::{StreamExt as _, TryStreamExt as _};
        if self.done || self.until.is_some_and(|end| self.next >= end) {
            return Ok((None, self.next, true));
        }
        let limit = match self.until {
            Some(end) => u32::try_from((end - self.next).min(u64::from(REPLAY_PAGE)))
                .map_err(|error| StreamError::LimitExceeded.with_source(error))?,
            None => REPLAY_PAGE,
        };
        let page = match self.stream.read(self.next, limit).await {
            Ok(records) => {
                records
                    .take(limit as usize + 1)
                    .try_collect::<Vec<_>>()
                    .await?
            }
            Err(error) if error.category() == StreamCategory::NotFound && self.next == 0 => {
                Vec::new()
            }
            Err(error) => return Err(error),
        };
        if page.len() > limit as usize {
            return Err(
                StreamError::Unavailable.with_source(ReplayViolation::OversizedPage {
                    limit,
                    actual: page.len(),
                }),
            );
        }
        let mut next = self.next;
        for record in &page {
            if record.sequence != next || self.until.is_some_and(|end| next >= end) {
                return Err(
                    StreamError::Unavailable.with_source(ReplayViolation::Discontinuity {
                        expected: next,
                        actual: record.sequence,
                        end: self.until,
                    }),
                );
            }
            next = next.checked_add(1).ok_or_else(|| {
                StreamError::LimitExceeded.with_source(ReplayViolation::CursorExhausted)
            })?;
        }
        if let Some(end) = self.until.filter(|end| page.is_empty() && next < *end) {
            return Err(StreamError::Unavailable
                .with_source(ReplayViolation::IncompletePrefix { cursor: next, end }));
        }
        let done = page.is_empty() || self.until == Some(next);
        Ok(((!page.is_empty()).then_some(page), next, done))
    }

    /// The sequence after the last record returned.
    #[must_use]
    pub fn cursor(&self) -> u64 {
        self.next
    }
}

/// Transport callers keep this allocation instead of copying a domain wrapper.
#[cfg(any(
    not(target_arch = "wasm32"),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub fn new_identity_bytes() -> bytes::Bytes {
    bytes::Bytes::copy_from_slice(&new_identity())
}

/// Fresh provider identities share the same seeded entropy boundary.
pub fn new_identity() -> [u8; 16] {
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    {
        let mut bytes = [0; 16];
        if acyclic_native_runtime::testing::fill_seeded(&mut bytes) {
            return bytes;
        }
    }
    uuid::Uuid::new_v4().into_bytes()
}
