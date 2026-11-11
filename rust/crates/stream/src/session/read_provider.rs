//! Read-side provider adapter over the existing credited session.
use super::Client;
use crate::{
    BoxProviderFuture, CommitId, CommittedEnvelope, FollowRequest, FollowStream, Heads,
    HeadsRequest, IdempotencyKey, IdempotencyObservation, OperationClaims, ProviderContext,
    ReadRequest, RecordStream, StreamError, StreamProvider, WriteOutcome, WriteRequest,
};
use std::sync::Arc;

/// A read-only provider on one authenticated session. Clones share transport,
/// revision tracking and receive credit; this adapter adds no tasks or cache.
#[derive(Clone)]
pub struct ReadProvider {
    client: Client,
    claims: Arc<OperationClaims>,
}

impl Client {
    /// Use the session's default credential with generic stream readers.
    /// Keep this session's credential binding for the lifetime of its readers;
    /// replacing the principal requires a new provider and reader attachment.
    #[must_use]
    pub fn read_provider(&self) -> ReadProvider {
        ReadProvider {
            client: self.clone(),
            claims: Arc::new(OperationClaims::default()),
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl StreamProvider for ReadProvider {
    fn context(&self) -> ProviderContext<'_> {
        ProviderContext::new(&self.claims)
    }

    async fn heads(&self, request: HeadsRequest) -> Result<Heads, StreamError> {
        self.client.heads(request, 0).await
    }

    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.client.read(request).await
    }

    async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError> {
        self.client.follow(request).await
    }

    async fn inspect_idempotency(
        &self,
        _: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError> {
        Err(StreamError::Unsupported)
    }

    fn write(&self, _: WriteRequest) -> BoxProviderFuture<'_, Result<WriteOutcome, StreamError>> {
        Box::pin(async { Err(StreamError::Unsupported) })
    }

    async fn read_commit(&self, _: CommitId) -> Result<CommittedEnvelope, StreamError> {
        Err(StreamError::Unsupported)
    }
}
