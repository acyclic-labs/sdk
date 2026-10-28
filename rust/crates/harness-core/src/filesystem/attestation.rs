//! Per-request platform evidence at the one point every hosted write seals.
//!
//! A hosting platform whose physical provider admits a tenant write only with
//! evidence bound to that exact request composes its placement filesystem
//! over [`Attested`], with its [`Attestor`] and the admission its
//! [`super::hosting::Bound`] acts under. Every write of that tree (its root,
//! each command and every agent step) then passes through `Attested`, which
//! digests the sealed request, asks the attestor for its evidence, and issues
//! the write on the provider bound to that attestation. Such a provider
//! refuses any unattested write, so a placement missing the wrapper fails
//! closed. The attestation is a function of the sealed request
//! itself, so it lives with that value and nowhere else: an uncertain retry
//! resubmits identical bytes and so presents the identical attestation, and
//! it is gone when the request is. `Attested` keeps no per-request state. A
//! prepared native commit, whose final record arrives only after preparation,
//! is refused under an attestor, since no attestation could cover its bytes.
//! Providers that need no evidence ignore it.
//!
//! `Attested` implements the generated no-default [`acyclic_stream::ExplicitStreamProvider`]
//! contract. A new provider method, even one with a default, requires an
//! explicit implementation here at compile time.
use super::TurnAdmission;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::NativeReadMeter;
use acyclic_stream::{
    AppendOutcome, AppendRequest, BoxProviderFuture, ChildrenPage, ChildrenPageRequest, CommitId,
    CommitOutcome, CommitRequest, CommittedEnvelope, DurableReceipt, FollowStream, ForkReceipt,
    ForkRequest, IdempotencyKey, IdempotencyObservation, OperationClaims, ProviderPlatform,
    ReadRequest, RecordStream, StreamError, StreamPath, StreamProvider, StreamWatermarks,
};
#[cfg(not(target_arch = "wasm32"))]
use acyclic_stream::{NativeProviderFuture, NativeRecordStream, PreparedNativeCommit};
use bytes::Bytes;
use std::sync::Arc;

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
///
/// Evidence is a pure function of the attempt: the same admission and digest
/// always yield the identical attestation (for example a deterministic
/// signature over the digest), so an exact retry is attested identically
/// without anything being retained between attempts.
pub trait Attestor: ProviderPlatform + 'static {
    /// Evidence for `attempt`, identical for every attempt of the same
    /// sealed request.
    fn attest<'a>(
        &'a self,
        attempt: Attempt<'a>,
    ) -> BoxProviderFuture<'a, Result<Attestation, StreamError>>;
}

/// A provider that may require per-request evidence.
pub trait AttestedProvider: StreamProvider + Sized {
    /// This provider bound to one write's `attestation`, or `None` to write
    /// on `self` because it requires no evidence.
    ///
    /// # Errors
    /// When the attestation cannot authorize a write.
    fn bind(&self, attestation: &Attestation) -> Result<Option<Arc<Self>>, StreamError> {
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
        }
    }

    /// The unattested placement stream.
    #[must_use]
    pub const fn inner(&self) -> &Arc<P> {
        &self.inner
    }

    /// The one sealing point: the provider every write is issued on, bound to
    /// the attestation of its exact request's `digest`.
    async fn target(
        &self,
        digest: impl FnOnce() -> Result<[u8; 32], StreamError>,
    ) -> Result<Arc<P>, StreamError> {
        let Some(attestor) = &self.attestor else {
            return Ok(Arc::clone(&self.inner));
        };
        let attestation = attestor
            .attest(Attempt {
                admission: &self.admission,
                digest: digest()?,
            })
            .await?;
        Ok(self
            .inner
            .bind(&attestation)?
            .unwrap_or_else(|| Arc::clone(&self.inner)))
    }
}

fn commit_digest(request: &CommitRequest) -> Result<[u8; 32], StreamError> {
    acyclic_stream::request::commit_digest(&mut request.clone())
}

#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
impl<P: AttestedProvider + 'static> acyclic_stream::ExplicitStreamProvider for Attested<P> {
    fn operation_claims(&self) -> &OperationClaims {
        self.inner.operation_claims()
    }
    fn has_exclusive_process_owner(&self) -> bool {
        self.inner.has_exclusive_process_owner()
    }
    fn supports_deferred_durability(&self) -> bool {
        self.inner.supports_deferred_durability()
    }
    async fn durability_barrier(&self) -> Result<(), StreamError> {
        self.inner.durability_barrier().await
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_durability_barrier(
        &self,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, ()> {
        self.inner.native_durability_barrier(meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_native_durability_barrier(
        &self,
        meter: Arc<dyn NativeReadMeter>,
    ) -> std::io::Result<NativeProviderFuture<'_, ()>> {
        self.inner.prepare_native_durability_barrier(meter)
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
    async fn head(&self, path: StreamPath) -> Result<acyclic_stream::PathHead, StreamError> {
        self.inner.head(path).await
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
        self.target(|| acyclic_stream::request::append_digest(&request))
            .await?
            .append(request)
            .await
    }
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError> {
        self.target(|| acyclic_stream::request::fork_digest(&request))
            .await?
            .fork(request)
            .await
    }
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError> {
        self.inner.read(request).await
    }
    async fn follow(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> Result<FollowStream, StreamError> {
        self.inner.follow(request).await
    }
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        self.inner.children_page(request).await
    }
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError> {
        self.target(|| commit_digest(&request))
            .await?
            .commit(request)
            .await
    }
    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError> {
        self.inner.read_commit(commit_id).await
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        self.inner.native_inspect_idempotency(key, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_append(
        &self,
        request: AppendRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, AppendOutcome> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            match self
                .target(|| acyclic_stream::request::append_digest(&request))
                .await
            {
                Ok(target) => target.native_append(request, original).await,
                Err(error) => Ok(Err(error)),
            }
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_tail(
        &self,
        path: StreamPath,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        self.inner.native_tail(path, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_read(
        &self,
        request: ReadRequest,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        self.inner.native_read(request, meter)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        request: CommitRequest,
        deadline: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            match self.target(|| commit_digest(&request)).await {
                Ok(target) => target.native_commit(request, deadline, original).await,
                Err(error) => Ok(Err(error)),
            }
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn NativeReadMeter>,
        before: F,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        Self: Sized,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let original = Arc::clone(&meter);
        NativeProviderFuture::new(&meter, async move {
            match self.target(|| commit_digest(&request)).await {
                Ok(target) => {
                    target
                        .native_commit_guarded(request, deadline_unix_millis, original, before)
                        .await
                }
                Err(error) => Ok(Err(E::from(error))),
            }
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        meter: Arc<dyn NativeReadMeter>,
    ) -> NativeProviderFuture<'_, PreparedNativeCommit> {
        // A prepared commit's final record arrives only at `finish`, after
        // this call, so no attestation here could cover the bytes the provider
        // publishes. Attested hosting refuses the path, as a provider without
        // it does; callers take the ordinary commit, which is attested whole.
        if self.attestor.is_some() {
            return NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)));
        }
        self.inner
            .native_prepare_commit(request, maximum_record_bytes, meter)
    }
}

#[cfg(test)]
mod tests {
    /// Counts each request for evidence.
    #[derive(Default)]
    struct Counting(
        std::sync::atomic::AtomicUsize,
        std::sync::Mutex<Vec<super::TurnAdmission>>,
    );

    impl super::Attestor for Counting {
        fn attest<'a>(
            &'a self,
            attempt: super::Attempt<'a>,
        ) -> super::BoxProviderFuture<'a, Result<super::Attestation, super::StreamError>> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.1
                .lock()
                .expect("admission ledger")
                .push(attempt.admission.clone());
            Box::pin(std::future::ready(Ok(super::Attestation(
                attempt.digest.to_vec().into(),
            ))))
        }
    }

    /// A provider recording which binding each native commit reached.
    #[cfg(not(target_arch = "wasm32"))]
    #[derive(Default)]
    struct Probe {
        inner: acyclic_stream::MemoryStream,
        bound: Option<super::Attestation>,
        reached: std::sync::Arc<std::sync::Mutex<Vec<Option<super::Attestation>>>>,
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl Probe {
        fn write(&self, digest: [u8; 32]) -> super::StreamError {
            assert_eq!(
                self.bound,
                Some(super::Attestation(digest.to_vec().into())),
                "write must reach the binding for its own canonical digest"
            );
            self.reached
                .lock()
                .expect("probe ledger")
                .push(self.bound.clone());
            super::StreamError::AccessDenied
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    impl super::AttestedProvider for Probe {
        fn bind(
            &self,
            attestation: &super::Attestation,
        ) -> Result<Option<std::sync::Arc<Self>>, super::StreamError> {
            Ok(Some(std::sync::Arc::new(Self {
                inner: acyclic_stream::MemoryStream::default(),
                bound: Some(attestation.clone()),
                reached: self.reached.clone(),
            })))
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[async_trait::async_trait]
    impl super::StreamProvider for Probe {
        fn operation_claims(&self) -> &super::OperationClaims {
            self.inner.operation_claims()
        }
        async fn inspect_idempotency(
            &self,
            key: super::IdempotencyKey,
        ) -> Result<Option<super::IdempotencyObservation>, super::StreamError> {
            self.inner.inspect_idempotency(key).await
        }
        async fn tail(&self, path: super::StreamPath) -> Result<u64, super::StreamError> {
            self.inner.tail(path).await
        }
        async fn head(
            &self,
            path: super::StreamPath,
        ) -> Result<acyclic_stream::PathHead, super::StreamError> {
            self.inner.head(path).await
        }
        async fn append(
            &self,
            request: super::AppendRequest,
        ) -> Result<super::AppendOutcome, super::StreamError> {
            Err(self.write(acyclic_stream::request::append_digest(&request)?))
        }
        async fn fork(
            &self,
            request: super::ForkRequest,
        ) -> Result<super::ForkReceipt, super::StreamError> {
            Err(self.write(acyclic_stream::request::fork_digest(&request)?))
        }
        async fn read(
            &self,
            request: super::ReadRequest,
        ) -> Result<super::RecordStream, super::StreamError> {
            self.inner.read(request).await
        }
        async fn follow(
            &self,
            request: acyclic_stream::FollowRequest,
        ) -> Result<acyclic_stream::FollowStream, super::StreamError> {
            self.inner.follow(request).await
        }
        async fn commit(
            &self,
            request: super::CommitRequest,
        ) -> Result<super::CommitOutcome, super::StreamError> {
            Err(self.write(super::commit_digest(&request)?))
        }
        async fn read_commit(
            &self,
            id: super::CommitId,
        ) -> Result<super::CommittedEnvelope, super::StreamError> {
            self.inner.read_commit(id).await
        }
        fn native_commit(
            &self,
            request: super::CommitRequest,
            _deadline: Option<u64>,
            _meter: std::sync::Arc<dyn super::NativeReadMeter>,
        ) -> super::NativeProviderFuture<'_, super::CommitOutcome> {
            // A definite answer distinct from the trait default's Unsupported.
            let result = super::commit_digest(&request).map(|digest| self.write(digest));
            super::NativeProviderFuture::ready(Ok(Err(result.unwrap_or_else(|error| error))))
        }
        fn native_append(
            &self,
            request: super::AppendRequest,
            _: std::sync::Arc<dyn super::NativeReadMeter>,
        ) -> super::NativeProviderFuture<'_, super::AppendOutcome> {
            let result =
                acyclic_stream::request::append_digest(&request).map(|digest| self.write(digest));
            super::NativeProviderFuture::ready(Ok(Err(result.unwrap_or_else(|error| error))))
        }
        fn native_commit_guarded<E, F, Fut>(
            &self,
            request: super::CommitRequest,
            _: Option<u64>,
            _: std::sync::Arc<dyn super::NativeReadMeter>,
            _before: F,
        ) -> super::NativeProviderFuture<'_, super::CommitOutcome, E>
        where
            Self: Sized,
            E: From<super::StreamError> + Send + 'static,
            F: FnMut() -> Fut + Send + 'static,
            Fut: std::future::Future<Output = Result<(), E>> + Send + 'static,
        {
            let result = super::commit_digest(&request).map(|digest| self.write(digest));
            super::NativeProviderFuture::ready(Ok(Err(E::from(
                result.unwrap_or_else(|error| error),
            ))))
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_native_write_is_attested_and_reaches_the_bound_provider()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_stream::{
            CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath,
        };
        let attestor = std::sync::Arc::new(Counting::default());
        let probe = std::sync::Arc::new(Probe::default());
        let reached = probe.reached.clone();
        let attested = super::Attested::new(
            probe,
            Some(attestor.clone()),
            super::TurnAdmission::LocalRoot,
        );
        let path = StreamPath::new("attested/native")?;
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![CommitCondition::Tail {
                path: path.clone(),
                expected: 0,
            }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![b"native".as_slice().into()],
            }],
            idempotency_key: IdempotencyKey::new(b"native".to_vec())?,
        };
        let digest = super::commit_digest(&request)?;
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(error, _)| error.to_string())?;
        let outcome =
            super::StreamProvider::native_commit(&attested, request, None, work.runtime_meter())
                .await?;
        assert!(matches!(outcome, Err(super::StreamError::AccessDenied)));
        assert_eq!(attestor.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert_eq!(
            *reached.lock().map_err(|_| "poisoned")?,
            vec![Some(super::Attestation(digest.to_vec().into()))],
            "the native commit reached the provider bound to its attestation"
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn a_prepared_native_commit_is_refused_under_an_attestor()
    -> Result<(), Box<dyn std::error::Error>> {
        use acyclic_stream::{
            CommitCondition, CommitMutation, CommitRequest, IdempotencyKey, StreamPath,
        };
        let attestor = std::sync::Arc::new(Counting::default());
        let attested = super::Attested::new(
            std::sync::Arc::new(Probe::default()),
            Some(attestor.clone()),
            super::TurnAdmission::LocalRoot,
        );
        let path = StreamPath::new("attested/prepared")?;
        let request = CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![CommitCondition::Tail {
                path: path.clone(),
                expected: 0,
            }],
            mutations: vec![CommitMutation::Append {
                path,
                records: vec![b"".as_slice().into()],
            }],
            idempotency_key: IdempotencyKey::new(b"prepared".to_vec())?,
        };
        let work = acyclic_fs::NativeMaterializationWork::new(
            acyclic_fs::WorkBudget::UNBOUNDED,
            &acyclic_fs::CancellationToken::new(),
        )
        .map_err(|(error, _)| error.to_string())?;
        let prepared = super::StreamProvider::native_prepare_commit(
            &attested,
            request,
            64,
            work.runtime_meter(),
        )
        .await?;
        assert!(matches!(prepared, Err(super::StreamError::Unsupported)));
        assert_eq!(
            attestor.0.load(std::sync::atomic::Ordering::SeqCst),
            0,
            "nothing was attested for bytes it could not cover"
        );
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    include!("attestation_proofs.rs");
}
