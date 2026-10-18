use acyclic_stream::*;
use futures::StreamExt;
use std::sync::Arc;

// Immutable retention floor models a provider that has removed a prefix; retained
// records and checked-anchor comparison still use the real MemoryStream path.
pub struct History {
    pub inner: Arc<MemoryStream>,
    pub floor: u64,
    pub unchecked: bool,
    pub skip: u64,
    pub stale_head: bool,
    pub gate: Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>, bool)>,
}
impl History {
    pub fn new(inner: Arc<MemoryStream>) -> Self {
        Self {
            inner,
            floor: 0,
            unchecked: false,
            skip: 0,
            stale_head: false,
            gate: None,
        }
    }
}
#[async_trait::async_trait]
impl StreamProvider for History {
    async fn inspect_idempotency(
        &self,
        key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        self.inner.inspect_idempotency(key).await
    }
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError> {
        self.inner.tail(path).await
    }
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError> {
        self.inner.bounds(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }
    async fn read(&self, mut request: ReadRequest) -> Result<RecordStream, StreamError> {
        // Negative control: serve an old live head after the terminal fact.
        if self.stale_head {
            request.from = 1;
        }
        self.inner.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.inner.follow(path, from).await
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.inner.commit(request).await
    }
    async fn read_commit(&self, id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(id).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        anchor: CommitId,
    ) -> Result<RecordStream, StreamError> {
        if self.unchecked {
            let tail = self.inner.tail(path.clone()).await?;
            return self.inner.follow(path, from.min(tail)).await;
        }
        if from - 1 < self.floor {
            return Err(StreamError::PrefixNotRetained);
        }
        let mut opened = None;
        if let Some((entered, release, after)) = &self.gate {
            if *after {
                opened = Some(
                    self.inner
                        .follow_checked(path.clone(), from, anchor)
                        .await?,
                );
            }
            entered.wait().await;
            release.wait().await;
        }
        let records = match opened {
            Some(records) => records,
            None => self.inner.follow_checked(path, from, anchor).await?,
        };
        Ok(Box::pin(records.skip(self.skip as usize)))
    }
}

// Deliberately broken provider variants used only as negative controls.
pub struct Broken {
    pub inner: Arc<dyn session::PublicationProvider>,
    pub skip_deletion: bool,
    pub skip_authorization: bool,
}
impl session::PublicationProvider for Broken {
    fn subscribe(
        &self,
        encoded: bytes::Bytes,
        resume: wire::SessionCursor,
    ) -> Result<session::PublicationSubscription, StreamError> {
        let mut source = self.inner.subscribe(encoded, resume)?;
        if self.skip_authorization {
            source.revalidate = Arc::new(|| Ok(()));
        }
        if self.skip_deletion {
            source.terminal = Box::pin(futures::future::pending());
            let open = source.open;
            source.open = Box::pin(async move {
                let records = open.await?;
                Ok(Box::pin(records.filter_map(|item| {
                    futures::future::ready(if matches!(item, Err(StreamError::NotFound)) {
                        None
                    } else {
                        Some(item)
                    })
                })) as session::PublicationStream)
            });
        }
        Ok(source)
    }
}

// The publication-race barrier belongs to the data registration only. Control
// observation uses the same authority through an ungated production provider.
#[cfg(not(acyclic_loom))]
pub struct SeparateTerminal {
    pub data: Arc<dyn session::PublicationProvider>,
    pub control: Arc<dyn session::PublicationProvider>,
}
#[cfg(not(acyclic_loom))]
impl session::PublicationProvider for SeparateTerminal {
    fn subscribe(
        &self,
        encoded: bytes::Bytes,
        resume: wire::SessionCursor,
    ) -> Result<session::PublicationSubscription, StreamError> {
        let mut data = self.data.subscribe(encoded.clone(), resume.clone())?;
        data.terminal = self.control.subscribe(encoded, resume)?.terminal;
        Ok(data)
    }
}
