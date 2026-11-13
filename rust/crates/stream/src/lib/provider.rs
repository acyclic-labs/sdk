/// Heap-allocated provider future with the platform's executor bound.
#[cfg(not(target_arch = "wasm32"))]
pub type BoxProviderFuture<'a, T> = futures::future::BoxFuture<'a, T>;
/// Heap-allocated provider future retaining browser event-loop ownership.
#[cfg(target_arch = "wasm32")]
pub type BoxProviderFuture<'a, T> = futures::future::LocalBoxFuture<'a, T>;

/// Builds and erases `create`'s future at a layering boundary.
///
/// The future is built and boxed in this never-inlined frame, so a caller's
/// layout, optimisation and auto-trait proof hold only the pointer: each
/// layer's type and compile-time depth stay constant whatever the layer below
/// does. A bare `Box::pin` is no such boundary, as it inlines into its caller.
#[inline(never)]
pub fn erase_provider_future<'a, F>(create: impl FnOnce() -> F) -> BoxProviderFuture<'a, F::Output>
where
    F: std::future::Future + ProviderTask + 'a,
{
    Box::pin(create())
}

/// Bound for provider futures submitted to the platform executor.
/// A formatter retained over a suspension cannot satisfy the native contract:
///
/// ```compile_fail
/// fn read() -> impl std::future::Future<Output = ()> + acyclic_stream::ProviderTask {
///     async {
///         let owner = 7;
///         let label = format_args!("{owner}");
///         std::future::pending::<()>().await;
///         std::hint::black_box(label);
///     }
/// }
/// ```
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
    /// Trusted in-process results use the caller's result budget; encoded-size
    /// protection is enforced only when the view crosses a transport boundary.
    /// For child pages, `after` without `hierarchy_version` is a valid exclusive
    /// seek within the current snapshot; providers must accept it.
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
        acyclic_observability::operation!(
            "acyclic.stream.durability_barrier",
            StreamError::code,
            || Err(StreamError::Unsupported)
        )
    }

    /// Reserves `root` as an ephemeral subtree held by the returned lease.
    /// `root` must be absent, outside every other lease, beneath a present
    /// parent (or top-level). The lease owns `root`'s subtree and every path
    /// it adopts: a commit publishing into the lease may also publish paths
    /// it creates, each beneath a present parent or one the commit creates,
    /// and the topmost created path of each joins the lease with its subtree.
    /// A write publishing only into owned paths is atomic, conditioned,
    /// idempotent and observable like any other, but lives only in this
    /// process's memory: never journalled or flushed, never listed by a
    /// children page, inherited by a durable fork, or in a snapshot. A write
    /// retains its account-scoped idempotency identity while its lease lives:
    /// `inspect_idempotency` and `read_commit` return the original result, and
    /// a different request cannot spend the same key in another lease or in
    /// durable storage. Releasing the lease forgets its results and keys;
    /// durable receipts remain available. A write that publishes durably may
    /// name no owned path at all, so it is refused
    /// with `InvalidArgument`, as is one that also writes an existing durable
    /// path or another lease's; a fresh path without a parent is `NotFound`.
    /// Dropping or settling the lease removes every subtree it owns, and
    /// their followers then observe `NotFound`; process death removes them
    /// too. Providers that cannot hold one in process report `Unsupported`.
    async fn lease_ephemeral(&self, _root: StreamPath) -> Result<EphemeralLease, StreamError> {
        acyclic_observability::operation!(
            "acyclic.stream.lease_ephemeral",
            StreamError::code,
            || Err(StreamError::Unsupported)
        )
    }

    /// Whether a live lease owns `path`, so a write to it is never durable.
    /// A provider without leases owns none.
    async fn is_ephemeral(&self, _path: &StreamPath) -> Result<bool, StreamError> {
        Ok(false)
    }

    /// Reads the retained terminal result for one caller-owned retry identity.
    async fn inspect_idempotency(
        &self,
        idempotency_key: IdempotencyKey,
    ) -> Result<Option<IdempotencyObservation>, StreamError>;
    /// Publishes one genuine request family without translating its retry identity.
    /// Dispatch chooses the concrete future before its sole heap allocation.
    /// For Commit requests, providers must normalize through
    /// [`request::validate_commit`](crate::request::validate_commit) before
    /// settling soft appends. Results, including skipped paths, use canonical
    /// path order produced by that validation, not caller mutation order.
    fn write(
        &self,
        request: WriteRequest,
    ) -> BoxProviderFuture<'_, std::result::Result<WriteOutcome, StreamError>>;
    /// Opens a bounded finite read pinned to one immutable history snapshot.
    /// Invalid limits or keys return LimitExceeded before revision waits or IO.
    /// The opening revision and bounds belong to that retained snapshot, even
    /// when it is empty or newer writes apply before the first item is polled.
    /// Opening a missing path returns NotFound; a start beyond that snapshot's
    /// tail returns OutOfRange. Neither refusal is deferred to the first item.
    async fn read(&self, request: ReadRequest) -> std::result::Result<RecordStream, StreamError>;
    /// Replays and remains live without a handoff gap. The history fence is checked
    /// atomically before subscription and on replacement. Ordered progress never
    /// passes an earlier matching record. Progress-enabled follows first yield
    /// `Opening` even at the tail; record-only follows retain indexed wakes.
    /// Selection, optional finite end and LATEST bootstrap are atomic with
    /// registration. Quiet and initially absent paths expose the opening receipt
    /// without waiting for an append. Finite cursors retain their original history.
    /// Loss of a pending snapshot producer ends the follow with
    /// [`StreamError::Unavailable`]; it does not silently restart the read.
    /// Native in-process snapshot reads that cannot finish immediately require
    /// an active Tokio runtime; without one they refuse before scheduling work.
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

/// A lease's pending release, taken exactly once.
type Release = Option<BoxProviderFuture<'static, Result<(), StreamError>>>;

/// Custody of one ephemeral subtree; see [`StreamProvider::lease_ephemeral`].
/// Dropping it releases the subtree without blocking.
#[must_use = "dropping the lease releases its subtree"]
pub struct EphemeralLease {
    root: StreamPath,
    // Only ever reached through `&mut self`, so the lock is never contended:
    // it makes custody shareable across threads, as every owner needs.
    release: std::sync::Mutex<Release>,
}

impl EphemeralLease {
    /// A lease whose `release` removes the subtree at `root` exactly once.
    pub fn new(
        root: StreamPath,
        release: BoxProviderFuture<'static, Result<(), StreamError>>,
    ) -> Self {
        Self {
            root,
            release: std::sync::Mutex::new(Some(release)),
        }
    }

    /// The leased subtree root.
    #[must_use]
    pub fn root(&self) -> &StreamPath {
        &self.root
    }

    /// Releases the subtree and waits until it is gone.
    pub async fn settle(mut self) -> Result<(), StreamError> {
        match take_release(&mut self.release) {
            Some(release) => release.await,
            None => Ok(()),
        }
    }
}

fn take_release(release: &mut std::sync::Mutex<Release>) -> Release {
    release
        .get_mut()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
}

impl std::fmt::Debug for EphemeralLease {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EphemeralLease")
            .field("root", &self.root)
            .finish_non_exhaustive()
    }
}

impl Drop for EphemeralLease {
    fn drop(&mut self) {
        #[cfg(any(not(target_arch = "wasm32"), feature = "wasm"))]
        if let Some(release) = take_release(&mut self.release) {
            crate::runtime::PlatformExecutor
                .spawn_owned(futures::FutureExt::map(release, |_released| ()));
        }
    }
}
