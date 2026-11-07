/// Optional native admission and physical completion capability.
///
/// The portable eight-operation contract does not imply this capability. A
/// native consumer must require it explicitly; it cannot silently substitute
/// ordinary work for original-owner funding. The default methods refuse before
/// any action. Implementations share publication decisions and pinned history
/// with their portable operations while retaining the original work and guard.
#[cfg(not(target_arch = "wasm32"))]
#[acyclic_contract_derive::explicit_provider]
pub trait NativeStreamProvider: StreamProvider {
    /// The same barrier, with admission and physical I/O on the original ledger.
    fn native_durability_barrier(
        &self,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, ()> {
        NativeProviderFuture::ready_observed(
            crate::native::span!("acyclic.stream.durability_barrier"),
            Ok(Err(StreamError::Unsupported)),
            StreamError::code,
        )
    }
    /// Reserves a phase's completion before writes are deferred. Polling seals
    /// the then-current accepted prefix without admitting new caller work.
    fn prepare_native_durability_barrier(
        &self,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<NativeProviderFuture<'_, ()>> {
        acyclic_observability::operation!(
            "acyclic.stream.durability_barrier",
            |_: &std::io::Error| "unsupported",
            || Err(std::io::ErrorKind::Unsupported.into())
        )
    }
    /// Canonical inspection funded by the original native work owner.
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let _ = key;
        NativeProviderFuture::ready_observed(
            crate::native::span!("acyclic.stream.inspect_idempotency"),
            Ok(Err(StreamError::Unsupported)),
            StreamError::code,
        )
    }
    /// Canonical tail observation on the original ledger.
    fn native_tail(
        &self,
        path: StreamPath,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        let _ = path;
        NativeProviderFuture::ready_observed(
            crate::native::span!("acyclic.stream.tail"),
            Ok(Err(StreamError::Unsupported)),
            StreamError::code,
        )
    }
    /// Coherent named heads and absence on the original ledger.
    ///
    /// Memory and Local support named `Tail` and `History` without durability
    /// confirmation, retained-record or child selections. History shares original
    /// record owners and funds reconstructed provenance on this same ledger.
    /// Its durable offset is zero, making no persistence claim. Other selections refuse with
    /// `Unsupported`; a native consumer must never fall back to ordinary work.
    /// The returned snapshot binds every selected offset and missing path.
    fn native_heads(
        &self,
        request: HeadsRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Heads> {
        let _ = request;
        NativeProviderFuture::ready_observed(
            crate::native::span!("acyclic.stream.heads"),
            Ok(Err(StreamError::Unsupported)),
            StreamError::code,
        )
    }
    /// Canonical finite read; original causes and output custody are retained.
    fn native_read(
        &self,
        request: ReadRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        let _ = request;
        NativeProviderFuture::ready_observed(
            crate::native::span!("acyclic.stream.read"),
            Ok(Err(StreamError::Unsupported)),
            StreamError::code,
        )
    }
    /// Publishes a genuine request family on its original native work owner.
    /// The Commit request carries its deadline independently of its retry digest.
    fn native_write(
        &self,
        request: WriteRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, WriteOutcome> {
        let span = match &request {
            WriteRequest::Append(_) => crate::native::span!("acyclic.stream.append"),
            WriteRequest::Fork(_) => crate::native::span!("acyclic.stream.fork"),
            WriteRequest::Commit(_) | WriteRequest::CommitAccepted(_) => {
                crate::native::span!("acyclic.stream.commit")
            }
        };
        let _ = request;
        NativeProviderFuture::ready_observed(
            span,
            Ok(Err(StreamError::Unsupported)),
            StreamError::code,
        )
    }
    /// Executes one canonical native Commit with its owned original guard.
    /// A cached success skips the guard. Otherwise the guard runs once, outside
    /// publication locks; a changed canonical cut returns `PrefixNotRetained`
    /// without running the guard again. A fresh invocation may try a new cut.
    /// Callers with a typed guard use [`NativeStreamProviderExt::native_commit_with_guard`].
    fn native_commit_guarded(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        before: NativeCommitGuard,
    ) -> NativeProviderFuture<'_, CommitOutcome, NativeCommitError> {
        let _ = (request, deadline_unix_millis, before);
        NativeProviderFuture::ready_observed(
            crate::native::span!("acyclic.stream.commit"),
            Ok(Err(NativeCommitError::Stream(StreamError::Unsupported))),
            |_| "unsupported",
        )
    }
    /// Funds the canonical two-tail/one-append/one-record Commit ingress before physical action.
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, PreparedNativeCommit> {
        let _ = (request, maximum_record_bytes);
        NativeProviderFuture::ready_observed(
            crate::native::span!("acyclic.stream.prepare_commit"),
            Ok(Err(StreamError::Unsupported)),
            StreamError::code,
        )
    }
}

/// The owned guard a native Commit runs before its effect.
///
/// It is erased so the provider contract stays object safe: a caller's guard
/// type never reaches a provider's type, and providers compose as
/// `dyn NativeStreamProvider`. The guard is boxed once per Commit.
#[cfg(not(target_arch = "wasm32"))]
pub type NativeCommitGuard =
    Box<dyn FnMut() -> BoxProviderFuture<'static, Result<(), NativeCommitError>> + Send>;

/// A guard's refusal, carried unchanged from the guard back to the caller
/// that built it. Only [`NativeStreamProviderExt::native_commit_with_guard`] creates one.
#[cfg(not(target_arch = "wasm32"))]
pub struct NativeGuardRefusal(Box<dyn std::any::Any + Send>);

#[cfg(not(target_arch = "wasm32"))]
impl std::fmt::Debug for NativeGuardRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("NativeGuardRefusal")
    }
}

/// Why a guarded native Commit failed.
#[cfg(not(target_arch = "wasm32"))]
#[derive(Debug)]
pub enum NativeCommitError {
    /// The provider refused or failed the Commit.
    Stream(StreamError),
    /// The caller's guard refused before the effect.
    Guard(NativeGuardRefusal),
}

#[cfg(not(target_arch = "wasm32"))]
impl From<StreamError> for NativeCommitError {
    fn from(error: StreamError) -> Self {
        Self::Stream(error)
    }
}

/// Typed guards over [`NativeStreamProvider::native_commit_guarded`].
#[cfg(not(target_arch = "wasm32"))]
pub trait NativeStreamProviderExt: NativeStreamProvider {
    /// Runs a guarded native Commit with a typed guard and returns the guard's
    /// own error unchanged. A refusal this guard did not raise is a provider
    /// fault, reported as [`StreamError::InvalidResponse`].
    fn native_commit_with_guard<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        mut before: F,
    ) -> impl Future<Output = NativeStreamResult<CommitOutcome, E>> + Send + '_
    where
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let guard: NativeCommitGuard = Box::new(move || {
            let refusal = before();
            Box::pin(async move {
                refusal
                    .await
                    .map_err(|error| NativeCommitError::Guard(NativeGuardRefusal(Box::new(error))))
            })
        });
        let commit = self.native_commit_guarded(request, deadline_unix_millis, meter, guard);
        async move {
            commit.await.map(|result| {
                result.map_err(|error| match error {
                    NativeCommitError::Stream(error) => E::from(error),
                    NativeCommitError::Guard(NativeGuardRefusal(refusal)) => refusal
                        .downcast::<E>()
                        .map_or_else(|_| E::from(StreamError::InvalidResponse), |error| *error),
                })
            })
        }
    }
}
#[cfg(not(target_arch = "wasm32"))]
impl<P: NativeStreamProvider + ?Sized> NativeStreamProviderExt for P {}
