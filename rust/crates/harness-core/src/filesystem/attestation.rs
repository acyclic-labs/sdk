//! Per-request platform evidence at the one point every hosted write seals.
//!
//! A hosting platform whose physical provider admits a tenant write only with
//! evidence bound to that exact request supplies an [`Attestor`]. Every write
//! of a [`super::hosting::Bound`] tree (its root, each command and every agent
//! step) passes through [`Attested`], which digests the sealed request, asks
//! the attestor once, and issues the write on the provider bound to that
//! attestation. The attestation is held with the request until a definite
//! outcome, so an uncertain retry resubmits identical bytes with the
//! identical attestation. Providers that need no evidence ignore it.
use super::TurnAdmission;
use acyclic_stream::{
    AppendOutcome, AppendRequest, BoxProviderFuture, ChildrenPage, ChildrenPageRequest, CommitId,
    CommitOutcome, CommitRequest, CommittedEnvelope, DurableReceipt, FollowFilter, FollowStream,
    ForkReceipt, ForkRequest, IdempotencyKey, IdempotencyObservation, OperationClaims,
    ProviderPlatform, ReadRequest, RecordStream, StreamBounds, StreamError, StreamPath,
    StreamProvider, StreamWatermarks,
};
use bytes::Bytes;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

/// Opaque platform evidence for one sealed request.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Attestation(pub Bytes);

/// One sealed write awaiting evidence.
#[derive(Clone, Copy, Debug)]
pub struct Attempt<'a> {
    /// The admission every write of this tree acts under: the original
    /// verified caller's scope and audience.
    pub admission: &'a TurnAdmission,
    /// The provider's canonical digest of the exact sealed request.
    pub digest: [u8; 32],
}

/// Supplies evidence for each sealed request of one hosted tree.
pub trait Attestor: ProviderPlatform + 'static {
    /// Evidence for `attempt`, requested once per distinct sealed request.
    fn attest<'a>(
        &'a self,
        attempt: Attempt<'a>,
    ) -> BoxProviderFuture<'a, Result<Attestation, StreamError>>;
}

/// A provider that may require per-request evidence.
pub trait AttestedProvider: StreamProvider {
    /// This provider bound to one write's `attestation`, or `None` to write
    /// on `self` because it requires no evidence.
    ///
    /// # Errors
    /// When the attestation cannot authorize a write.
    fn bind(
        &self,
        attestation: &Attestation,
    ) -> Result<Option<Arc<dyn StreamProvider>>, StreamError> {
        let _ = attestation;
        Ok(None)
    }
}

impl AttestedProvider for acyclic_stream::MemoryStream {}
#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
impl AttestedProvider for acyclic_stream::LocalStream {}

/// The placement stream with each write attested; reads pass through.
pub struct Attested<P> {
    inner: Arc<P>,
    attestor: Option<Arc<dyn Attestor>>,
    admission: TurnAdmission,
    /// Attestations of requests without a definite outcome yet.
    held: Mutex<HashMap<[u8; 32], Attestation>>,
}

impl<P: AttestedProvider + 'static> Attested<P> {
    /// `inner` with every write attested by `attestor` under `admission`;
    /// without an attestor, writes go straight to `inner`.
    #[must_use]
    pub fn new(
        inner: Arc<P>,
        attestor: Option<Arc<dyn Attestor>>,
        admission: TurnAdmission,
    ) -> Self {
        Self {
            inner,
            attestor,
            admission,
            held: Mutex::new(HashMap::new()),
        }
    }

    /// The unattested placement stream.
    #[must_use]
    pub const fn inner(&self) -> &Arc<P> {
        &self.inner
    }

    /// The attested target of the write digested by `digest`.
    async fn target(
        &self,
        digest: impl FnOnce() -> Result<[u8; 32], StreamError>,
    ) -> Result<Option<(Option<Arc<dyn StreamProvider>>, [u8; 32])>, StreamError> {
        let Some(attestor) = &self.attestor else {
            return Ok(None);
        };
        let digest = digest()?;
        let held = self.lock()?.get(&digest).cloned();
        let attestation = match held {
            Some(attestation) => attestation,
            None => {
                let attestation = attestor
                    .attest(Attempt {
                        admission: &self.admission,
                        digest,
                    })
                    .await?;
                self.lock()?.entry(digest).or_insert(attestation).clone()
            }
        };
        Ok(Some((self.inner.bind(&attestation)?, digest)))
    }

    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, HashMap<[u8; 32], Attestation>>, StreamError> {
        self.held.lock().map_err(|_| StreamError::Unavailable)
    }

    /// Releases the attestation of `digest` once its outcome is definite.
    fn settle<T>(&self, digest: Option<[u8; 32]>, outcome: &Result<T, StreamError>) {
        let uncertain = matches!(
            outcome,
            Err(StreamError::Unavailable
                | StreamError::DeadlineElapsed
                | StreamError::InvalidResponse)
        );
        if let (Some(digest), false) = (digest, uncertain)
            && let Ok(mut held) = self.held.lock()
        {
            held.remove(&digest);
        }
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P: AttestedProvider + 'static> StreamProvider for Attested<P> {
    fn operation_claims(&self) -> &OperationClaims {
        self.inner.operation_claims()
    }
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
    async fn durable_receipt(
        &self,
        path: StreamPath,
        through: u64,
    ) -> Result<Option<DurableReceipt>, StreamError> {
        self.inner.durable_receipt(path, through).await
    }
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError> {
        let Some((bound, digest)) = self
            .target(|| acyclic_stream::request::append_digest(&request))
            .await?
        else {
            return self.inner.append(request).await;
        };
        let outcome = match bound {
            Some(bound) => bound.append(request).await,
            None => self.inner.append(request).await,
        };
        self.settle(Some(digest), &outcome);
        outcome
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        let Some((bound, digest)) = self
            .target(|| acyclic_stream::request::fork_digest(&request))
            .await?
        else {
            return self.inner.fork(request).await;
        };
        let outcome = match bound {
            Some(bound) => bound.fork(request).await,
            None => self.inner.fork(request).await,
        };
        self.settle(Some(digest), &outcome);
        outcome
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        path: StreamPath,
        from: u64,
        filter: FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.inner.follow(path, from, filter).await
    }
    async fn follow_checked(
        &self,
        path: StreamPath,
        from: u64,
        preceding: CommitId,
        filter: FollowFilter,
    ) -> Result<RecordStream, StreamError> {
        self.inner
            .follow_checked(path, from, preceding, filter)
            .await
    }
    async fn follow_updates(
        &self,
        path: StreamPath,
        from: u64,
        preceding: Option<CommitId>,
        filter: FollowFilter,
    ) -> Result<FollowStream, StreamError> {
        self.inner
            .follow_updates(path, from, preceding, filter)
            .await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.inner.children_page(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        let Some((bound, digest)) = self
            .target(|| acyclic_stream::request::commit_digest(&mut request.clone()))
            .await?
        else {
            return self.inner.commit(request).await;
        };
        let outcome = match bound {
            Some(bound) => bound.commit(request).await,
            None => self.inner.commit(request).await,
        };
        self.settle(Some(digest), &outcome);
        outcome
    }
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        let Some((bound, digest)) = self
            .target(|| acyclic_stream::request::commit_digest(&mut request.clone()))
            .await?
        else {
            return self
                .inner
                .commit_before(request, deadline_unix_millis)
                .await;
        };
        let outcome = match bound {
            Some(bound) => bound.commit_before(request, deadline_unix_millis).await,
            None => {
                self.inner
                    .commit_before(request, deadline_unix_millis)
                    .await
            }
        };
        self.settle(Some(digest), &outcome);
        outcome
    }
    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit_id).await
    }
}
