//! Yields at provider contract boundaries; dependency-internal locks are not modeled.
use acyclic_stream::*;
use std::sync::Arc;

pub(super) struct BoundaryStream {
    pub inner: Arc<MemoryStream>,
    pub broken_follow: bool,
    pub owner: Option<(StreamPath, u64)>,
    pub broken_fence: bool,
    pub lose_result_ack: bool,
}

#[async_trait::async_trait]
impl StreamProvider for BoundaryStream {
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
    async fn watermarks(&self, path: StreamPath) -> Result<StreamWatermarks, StreamError> {
        self.inner.watermarks(path).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        self.inner.append(request).await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.inner.fork(request).await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError> {
        self.inner.follow(path, from).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
    ) -> Result<RecordStream, StreamError> {
        shuttle::future::yield_now().await;
        if self.broken_follow {
            self.inner
                .follow(path.clone(), self.inner.tail(path).await?)
                .await
        } else {
            self.inner.follow_checked(path, from, preceding).await
        }
    }
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError> {
        self.inner.children(request).await
    }
    async fn commit(&self, mut request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        shuttle::future::yield_now().await;
        let has_owner = self.owner.as_ref().is_some_and(|(path, _)| request.conditions.iter().any(|condition|
            matches!(condition, CommitCondition::Tail {path:condition_path, ..} if condition_path == path)));
        let stale = if let Some((path, expected)) = &self.owner {
            if self.broken_fence {
                request.conditions.retain(|condition| {
                    !matches!(condition,
                    CommitCondition::Tail {path:condition_path, ..} if condition_path == path)
                });
            }
            self.inner.tail(path.clone()).await? != *expected
        } else {
            false
        };
        // No scheduling point inside a provider's atomic transaction boundary.
        let outcome = self.inner.commit(request).await?;
        assert!(
            !stale || !matches!(outcome, CommitOutcome::Committed(_)),
            "stale task owner published a result"
        );
        if self.lose_result_ack && has_owner && matches!(outcome, CommitOutcome::Committed(_)) {
            return Err(StreamError::Unavailable);
        }
        Ok(outcome)
    }
    async fn read_commit(&self, commit: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit).await
    }
}
