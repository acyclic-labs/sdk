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

#[acyclic_contract_derive::explicit_provider]
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
/// Canonical provider contract. Placement and transport remain invisible.
pub trait StreamProvider: ProviderPlatform + 'static {
    /// Session ownership and backend publication policy.
    fn context(&self) -> ProviderContext<'_>;
    /// Observes named heads, absence and a child page atomically at one snapshot.
    /// Adapters must forward this operation intact; separate head calls cannot
    /// establish this guarantee. Denial refuses the whole view, never absence.
    async fn heads(&self, request: HeadsRequest) -> Result<Heads, StreamError>;
    /// Makes every write accepted by this session before the call durable.
    /// Visibility and live delivery do not wait for this barrier.
    async fn durability_barrier(&self) -> Result<(), StreamError> {
        Err(StreamError::Unsupported)
    }
    /// The same barrier, with admission and physical I/O on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    fn native_durability_barrier(
        &self,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, ()> {
        NativeProviderFuture::ready(Ok(Err(StreamError::Unsupported)))
    }
    /// Reserves a phase's completion before writes are deferred. Polling seals
    /// the then-current accepted prefix without admitting new caller work.
    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_native_durability_barrier(
        &self,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> std::io::Result<NativeProviderFuture<'_, ()>> {
        Err(std::io::ErrorKind::Unsupported.into())
    }
    /// Reads the retained terminal result for one caller-owned retry identity.
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError>;
    /// Publishes one genuine request family without translating its retry identity.
    /// Dispatch chooses the concrete future before its sole heap allocation.
    fn write(
        &self,
        request: WriteRequest,
    ) -> BoxProviderFuture<'_, std::result::Result<WriteOutcome, StreamError>>;
    /// Opens a bounded finite read pinned to one immutable history snapshot.
    /// Opening a missing path returns NotFound; a start beyond that snapshot's
    /// tail returns OutOfRange. Neither refusal is deferred to the first item.
    async fn read(&self, request: ReadRequest) -> Result<RecordStream, StreamError>;
    /// Replays and remains live without a handoff gap. The history fence is checked
    /// atomically before subscription and on replacement. Ordered progress never
    /// passes an earlier matching record; record-only follows retain indexed wakes.
    async fn follow(&self, request: FollowRequest) -> Result<FollowStream, StreamError>;

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
        deadline: Option<u64>,
        _meter: Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> NativeProviderFuture<'_, CommitOutcome> {
        let _ = (request, deadline);
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
        F: FnMut() -> Fut + Send + 'static,
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
}
