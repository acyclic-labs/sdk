use super::*;

/// Capabilities that cannot be inferred from storage trait syntax alone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EmbeddedCapabilities {
    /// Both authority and object acknowledgement survive backend restart.
    pub durable: bool,
}

impl EmbeddedCapabilities {
    /// Deterministic process-local memory profile.
    pub const MEMORY: Self = Self { durable: false };
}

pub(super) struct FsInner<A, O> {
    pub(super) authority: A,
    pub(super) objects: O,
    pub(super) capabilities: EmbeddedCapabilities,
    pub(super) workspace_namespace: [u8; 16],
    pub(super) path_index: crate::path_index::MemoryGenerationPathIndex,
    // This field must remain last: Rust drops fields in declaration order, so every durable
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(super) _local_root_lifecycle: Option<OwnershipAnchor>,
}

/// Embedded filesystem composition handle.
pub struct Fs<A, O> {
    pub(super) inner: Arc<FsInner<A, O>>,
}

impl<A, O> Clone for Fs<A, O> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<A, O> Fs<A, O> {

    /// Deployment guarantees supplied when this engine was composed.
    #[must_use]
    pub fn capabilities(&self) -> EmbeddedCapabilities {
        self.inner.capabilities
    }

    pub(crate) fn workspace_name_identity(
        &self, name: impl AsRef<str>, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<(crate::WorkspaceName, crate::WorkspaceId)>, WorkspaceError> {

        let named = crate::WorkspaceName::new(name, budget, cancellation)?;
        let work = named.work;
        let derived = crate::WorkspaceId::derive(
            self.inner.workspace_namespace, &named.value,
            work.remaining(budget).map_err(|error| OperationFailure::new(WorkspaceError::Work(error), work))?,
            cancellation,
        ).map_err(|failure| failure.map_with_prior_work(work, |error| error))?;
        let work = work.checked_add(derived.work).map_err(|error| OperationFailure::new(WorkspaceError::Work(error), work))?;
        Ok(FsReceipt { value: (named.value, derived.value), work })
    }

    /// Derives the deployment-scoped stable identity for one canonical name.
    pub fn workspace_id(
        &self,
        name: impl AsRef<str>,
    ) -> Result<crate::WorkspaceId, crate::WorkspaceNameError> {
        self.workspace_name_identity(name, WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map(|receipt| receipt.value.1).map_err(|failure| match failure.error {
                WorkspaceError::Name(error) => error, _ => crate::WorkspaceNameError::Allocation,
            })
    }

    pub(crate) fn same_deployment(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.inner, &other.inner)
    }

    /// Whether a workspace was acquired from this exact filesystem handle or its clones.
    #[must_use]
    pub fn owns_workspace(&self, workspace: &crate::Workspace<A, O>) -> bool {
        self.same_deployment(&workspace.volume.fs)
    }

    pub(crate) fn path_index(&self) -> &crate::path_index::MemoryGenerationPathIndex {
        &self.inner.path_index
    }

    pub(crate) fn authority(&self) -> &A {
        &self.inner.authority
    }

    /// Coordinates leases on an authority backend that stores windows atomically
    #[must_use]
    pub fn operation_windows(&self) -> crate::OperationWindowCoordinator<A>
    where
        A: crate::OperationWindowStore + Clone,
    {
        crate::OperationWindowCoordinator::new(self.inner.authority.clone())
    }

    pub(crate) fn objects(&self) -> &O {
        &self.inner.objects
    }

    /// Composes explicit authority and object backends without another state store.
    #[must_use]
    pub fn new(authority: A, objects: O, capabilities: EmbeddedCapabilities) -> Self {
        Self::new_in_namespace(authority, objects, capabilities, [0; 16])
    }

    /// Composes explicit backends in one stable workspace-identity namespace.
    #[must_use]
    pub fn new_in_namespace(
        authority: A,
        objects: O,
        capabilities: EmbeddedCapabilities,
        workspace_namespace: [u8; 16],
    ) -> Self {
        Self {
            inner: Arc::new(FsInner {
                authority,
                objects,
                capabilities,
                workspace_namespace,
                path_index: crate::path_index::MemoryGenerationPathIndex::default(),
                #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                _local_root_lifecycle: None,
            }),
        }
    }
}

#[cfg(feature = "distributed")]
impl<P, O> Fs<crate::StreamAuthorityStore<P>, O> {

    /// Opens the SDK operation-window store on the exact Stream provider used
    #[must_use]
    pub fn operation_window_store(&self) -> crate::StreamOperationWindowStore<P> {
        crate::StreamOperationWindowStore::new(self.inner.authority.provider())
    }
}

/// Durable local authority backend with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalAuthorityBackend =
    crate::distributed::StreamAuthorityStore<acyclic_stream::LocalStream>;

/// Durable local immutable-object backend with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalObjectBackend = crate::staged_objects::StagedObjects<
    crate::cache::CachedObjectStore<
        crate::LogicalObjectStore<acyclic_objects::v1::local::LocalObjects>,
    >,
>;

/// Durable local filesystem composition with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalFs = Fs<LocalAuthorityBackend, LocalObjectBackend>;

/// Durable operation-window store sharing the local filesystem's authority
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalOperationWindowStore = crate::StreamOperationWindowStore<acyclic_stream::LocalStream>;

/// Durable local volume handle with nonblocking native storage dispatch.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub type LocalVolume = Volume<LocalAuthorityBackend, LocalObjectBackend>;

/// Deterministic in-process composition over the exact public Stream and Objects providers.
#[cfg(all(feature = "memory", feature = "distributed"))]
pub type MemoryFs = Fs<MemoryAuthorityBackend, MemoryObjectBackend>;

#[cfg(all(feature = "memory", feature = "distributed"))]
/// Filesystem authority adapter backed by the public in-memory Stream provider.
pub type MemoryAuthorityBackend =
    crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>;

#[cfg(all(feature = "memory", feature = "distributed"))]
/// Filesystem object adapter backed by the public in-memory Objects provider.
pub type MemoryObjectBackend = crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>;

#[cfg(all(test, feature = "memory", feature = "distributed"))]
pub(crate) type MemoryCheckout = Checkout<MemoryAuthorityBackend, MemoryObjectBackend>;

impl<A, S> Fs<A, crate::cache::CachedObjectStore<S>> {

    /// Returns exact current and cumulative observations from the disposable
    pub fn object_cache_stats(&self) -> Result<crate::cache::ObjectCacheStats, ObjectStoreError> {
        self.inner.objects.stats()
    }

    /// Discards all resident accelerated bytes without changing authority or
    pub fn clear_object_cache(&self) -> Result<(), ObjectStoreError> {
        self.inner.objects.clear()
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl Fs<LocalAuthorityBackend, LocalObjectBackend> {

    /// Observes the disposable local immutable-object cache.
    pub fn object_cache_stats(&self) -> Result<crate::cache::ObjectCacheStats, ObjectStoreError> {
        self.inner.objects.inner().stats()
    }

    /// Drops cached bytes without changing persistent object storage.
    pub fn clear_object_cache(&self) -> Result<(), ObjectStoreError> {
        self.inner.objects.inner().clear()
    }
}

#[cfg(all(feature = "memory", feature = "distributed"))]
impl
    Fs<
        crate::distributed::StreamAuthorityStore<acyclic_stream::MemoryStream>,
        crate::LogicalObjectStore<acyclic_objects::v1::MemoryObjects>,
    >
{

    /// Creates the deterministic infrastructure-free composition from the exact public reference
    #[must_use]
    pub fn memory() -> Self {
        let stream = std::sync::Arc::new(acyclic_stream::MemoryStream::default());
        let (objects, bucket) = acyclic_objects::v1::MemoryObjects::with_default_bucket();
        Self::from_memory_providers(stream, std::sync::Arc::new(objects), bucket)
    }

    /// Composes Filesystem over caller-owned public reference providers.
    #[must_use]
    pub fn from_memory_providers(
        stream: std::sync::Arc<acyclic_stream::MemoryStream>,
        objects: std::sync::Arc<acyclic_objects::v1::MemoryObjects>,
        bucket: acyclic_objects::v1::wire::BucketRef,
    ) -> Self {
        Self::new(
            crate::distributed::StreamAuthorityStore::new(stream),
            crate::LogicalObjectStore::new(objects, bucket),
            EmbeddedCapabilities::MEMORY,
        )
    }
}

