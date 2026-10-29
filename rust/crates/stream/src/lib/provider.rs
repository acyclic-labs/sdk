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
/// Eight canonical provider operations. Placement and transport remain invisible.
/// Native original-work admission is an optional, separately required capability.
pub trait StreamProvider: ProviderPlatform + 'static {
    /// Session ownership and backend publication policy.
    fn context(&self) -> ProviderContext<'_>;
    /// Observes named heads, absence and a child page atomically at one snapshot.
    /// Adapters must forward this operation intact; separate head calls cannot
    /// establish this guarantee. Denial refuses the whole view, never absence.
    async fn heads(&self, request: HeadsRequest) -> Result<Heads, StreamError>;
    /// Exact Auth source used by this provider's transport, when configured.
    /// Forwarding adapters preserve it. Subscribe before resolving a lease or
    /// consulting source-bound cached state, then retain the resolved lease's
    /// binding and check through result admission. A source revision is never
    /// permission for a target. Raw callbacks have no trusted binding/watch;
    /// neither those nor an absent source imply a stable cache identity.
    fn credential_provider(&self) -> Option<&acyclic_auth::credential::CredentialProvider> {
        None
    }
    /// Makes every write accepted by this session before the call durable.
    /// Visibility and live delivery do not wait for this barrier.
    async fn durability_barrier(&self) -> Result<(), StreamError> {
        Err(StreamError::Unsupported)
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
    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError>;
    /// Replays and remains live without a handoff gap. The history fence is checked
    /// atomically before subscription and on replacement. Ordered progress never
    /// passes an earlier matching record; record-only follows retain indexed wakes.
    async fn follow(
        &self,
        request: FollowRequest,
    ) -> std::result::Result<FollowStream, StreamError>;

    /// Reads one complete immutable successful envelope.
    async fn read_commit(
        &self,
        commit_id: CommitId,
    ) -> std::result::Result<CommittedEnvelope, StreamError>;
}
