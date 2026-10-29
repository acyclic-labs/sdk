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
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }
    /// Reserves a phase's completion before writes are deferred. Polling seals
    /// the then-current accepted prefix without admitting new caller work.
    fn prepare_native_durability_barrier(
        &self,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<NativeProviderFuture<'_, ()>> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    /// Canonical inspection funded by the original native work owner.
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let _ = key;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }
    /// Canonical tail observation on the original ledger.
    fn native_tail(
        &self,
        path: StreamPath,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        let _ = path;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }
    /// Canonical finite read; original causes and output custody are retained.
    fn native_read(
        &self,
        request: ReadRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        let _ = request;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }
    /// Publishes a genuine request family on its original native work owner.
    /// The Commit request carries its deadline independently of its retry digest.
    fn native_write(
        &self,
        request: WriteRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, WriteOutcome> {
        let _ = request;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }
    /// Executes the canonical native Commit with an owned original guard before every protected publication attempt.
    fn native_commit_guarded<E, F, Fut>(
        &self,
        request: CommitRequest,
        deadline_unix_millis: Option<u64>,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
        before: F,
    ) -> NativeProviderFuture<'_, CommitOutcome, E>
    where
        Self: Sized,
        E: From<StreamError> + Send + 'static,
        F: FnMut() -> Fut + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let _ = (request, deadline_unix_millis, before);
        NativeProviderFuture::ready(Ok(Err(E::from(StreamError::Unsupported))))
    }
    /// Funds the canonical two-tail/one-append/one-record Commit ingress before physical action.
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, PreparedNativeCommit> {
        let _ = (request, maximum_record_bytes);
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }
}
