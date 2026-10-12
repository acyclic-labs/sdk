use super::*;

/// Heap-allocated provider future with the platform's executor bound.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxProviderFuture<'a, T> = futures::future::BoxFuture<'a, T>;
/// Heap-allocated provider future retaining browser event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub type BoxProviderFuture<'a, T> = futures::future::LocalBoxFuture<'a, T>;

/// Bound for provider futures submitted to the platform executor.
#[cfg(not(target_arch = "wasm32"))]
pub trait ProviderTask: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> ProviderTask for T {}
/// Browser executor futures retain event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub trait ProviderTask {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> ProviderTask for T {}
/// Heap-allocated provider cursor with the platform's executor bound.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxProviderStream<'a, T> = futures::stream::BoxStream<'a, T>;
/// Heap-allocated provider cursor retaining browser event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub type BoxProviderStream<'a, T> = futures::stream::LocalBoxStream<'a, T>;

/// Platform ownership boundary for native threads or browser event loops.
#[cfg(not(target_arch = "wasm32"))]
pub trait ProviderPlatform: Send + Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + Sync + ?Sized> ProviderPlatform for T {}
/// Browser providers may own thread-affine platform handles.
#[cfg(target_arch = "wasm32")]
pub trait ProviderPlatform {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> ProviderPlatform for T {}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
/// Canonical provider contract. Placement and transport remain invisible.
pub trait StreamProvider: ProviderPlatform + 'static {
    /// Reads the retained terminal result for one caller-owned retry identity.
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError>;
    /// Current next sequence.
    async fn tail(&self, path: StreamPath) -> Result<u64, StreamError>;
    /// Atomically observes both ends of the retained replay window.
    async fn bounds(&self, path: StreamPath) -> Result<StreamBounds, StreamError>;
    /// Atomic append or tail conflict.
    async fn append(&self, request: AppendRequest) -> Result<AppendOutcome, StreamError>;
    /// Atomic immutable-prefix fork.
    async fn fork(&self, request: ForkRequest) -> Result<ForkReceipt, StreamError>;
    /// Opens a bounded finite read.
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError>;
    /// Replays and then remains live without a handoff gap.
    async fn follow(&self, path: StreamPath, from: u64) -> Result<RecordStream, StreamError>;
    /// Lists one fixed-snapshot direct-child page.
    async fn children(&self, request: ChildrenRequest) -> Result<ChildStream, StreamError>;
    /// Traverses arbitrarily large direct-child sets without silently duplicating or omitting entries across concurrent hierarchy changes.
    async fn children_page(
        &self,
        request: ChildrenPageRequest,
    ) -> Result<ChildrenPage, StreamError> {
        let _ = request;
        Err(StreamError::Unsupported)
    }
    /// Executes one all-or-nothing optimistic commit.
    async fn commit(&self, request: CommitRequest) -> Result<CommitOutcome, StreamError>;
    /// Executes one coordinated commit only if the provider's trusted clock is strictly before `deadline_unix_millis` at the linearization point.
    async fn commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
    ) -> Result<CommitOutcome, StreamError> {
        let _ = (request, deadline_unix_millis);
        Err(StreamError::Unsupported)
    }
    /// Reads one complete immutable successful envelope.
    async fn read_commit(&self, commit_id: CommitId) -> Result<CommittedEnvelope, StreamError>;

    /// Canonical inspection funded by the original native work owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_inspect_idempotency(
        &self,
        key: IdempotencyKey,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, Option<IdempotencyObservation>> {
        let _ = key;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Executes the genuine append family on the original native work owner.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_append(
        &self,
        request: AppendRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, AppendOutcome> {
        let _ = request;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Canonical tail observation on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_tail(
        &self,
        path: StreamPath,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, u64> {
        let _ = path;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Canonical retained-window observation on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_bounds(
        &self,
        path: StreamPath,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, StreamBounds> {
        let _ = path;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Canonical finite read; original causes and output custody are retained.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_read(
        &self,
        request: ReadRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, NativeRecordStream> {
        let _ = request;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Canonical commit with allocation admission before its future frame.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit(
        &self,
        request: CommitRequest,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let _ = request;
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Executes the canonical native Commit with an owned original guard before every protected publication attempt.
    #[cfg(not(target_arch = "wasm32"))]
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
        F: FnMut() -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let _ = (request, deadline_unix_millis, before);
        NativeProviderFuture::ready(Ok(Err(E::from(StreamError::Unsupported))))
    }

    /// Funds the canonical two-tail/one-append/one-record Commit ingress before physical action.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_prepare_commit(
        &self,
        request: CommitRequest,
        maximum_record_bytes: usize,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, PreparedNativeCommit> {
        let _ = (request, maximum_record_bytes);
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

    /// Executes the same canonical commit with the provider's trusted clock checked at its actual linearization point, not at caller admission.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_commit_before(
        &self,
        request: CommitRequest,
        deadline_unix_millis: u64,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let _ = (request, deadline_unix_millis);
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }

}
