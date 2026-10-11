//! Explicit same-graph policy backed by the original Filesystem and lazy stores.

use super::{FilesystemHost, map_error};
use crate::contract::{JsonBounds, JsonCodecCharge, canonical_json_bytes_bounded};
use crate::conversation::{VolumeClass, VolumeRef};
use crate::core::Authority;
use crate::source_authorization::{ProjectSourceSnapshot, RootProjectSourceContext, RootProjectSourceProvider};
use crate::{Error, Result, TaskId};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, DemandSource, Digest, GenerationId, LazySnapshotRef, LazyWorkspace, LazyWorkspaceStore, WorkBudget, WorkCounters};
use serde::Deserialize;
use std::sync::Arc;

/// A retained original lazy source, not a descriptor-created namespace.
pub struct FsRootNativeSource<A, O> {
    backing: Arc<dyn RootLazyBacking<A, O>>,
}

impl<A, O> Clone for FsRootNativeSource<A, O> {
    fn clone(&self) -> Self { Self { backing: self.backing.clone() } }
}

fn root_source_error(source: acyclic_fs::LazyWorkspaceError) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    { Error::from(source) }
    #[cfg(target_arch = "wasm32")]
    { Error::Storage(source.to_string()) }
}
#[cfg(not(target_arch = "wasm32"))]
type RootLazyPublication<A, O> = (
    acyclic_fs::Generation<A, O>,
    std::result::Result<LazySnapshotRef, Error>,
);

#[cfg(not(target_arch = "wasm32"))]
fn root_lazy_mutations(mutations: &[super::WorkspaceMutation])
    -> impl Iterator<Item = acyclic_fs::LazyWriteMutation<'_>> + Send
{
    mutations.iter().filter_map(|mutation| match mutation {
        super::WorkspaceMutation::CreateDirectory { path } =>
            Some(acyclic_fs::LazyWriteMutation::CreateDirectory { path }),
        super::WorkspaceMutation::PutFile { path, bytes } =>
            Some(acyclic_fs::LazyWriteMutation::PutFile { path, bytes: bytes.clone() }),
        super::WorkspaceMutation::Remove { .. } => None,
    })
}

trait RootLazyBacking<A, O>: acyclic_stream::ProviderPlatform {
    fn workspace_id(&self) -> acyclic_fs::WorkspaceId;
    #[cfg(not(target_arch = "wasm32"))]
    fn matches_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool;
    fn validate<'a>(&'a self, snapshot: LazySnapshotRef, budget: WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken)
        -> acyclic_stream::BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>>;
    fn read<'a>(&'a self, snapshot: LazySnapshotRef, path: &'a str, maximum: u64,
        budget: WorkBudget, cancellation: &'a acyclic_fs::CancellationToken)
        -> acyclic_stream::BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_stream::OwnedBytes>, Error>>;
    fn list<'a>(&'a self, snapshot: LazySnapshotRef, path: &'a str,
        cursor: Option<acyclic_fs::LazyDirectoryCursor>, maximum: u32,
        budget: WorkBudget, cancellation: &'a acyclic_fs::CancellationToken)
        -> acyclic_stream::BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_fs::LazyDirectoryPage>, Error>>;
    #[cfg(not(target_arch = "wasm32"))]
    fn validate_native<'a>(&'a self, snapshot: LazySnapshotRef,
        work: &'a acyclic_fs::NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>>;
    #[cfg(not(target_arch = "wasm32"))]
    fn read_native<'a>(&'a self, snapshot: LazySnapshotRef, path: &'a str, maximum: u64,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>;
    #[cfg(not(target_arch = "wasm32"))]
    fn publish<'a>(&'a self, snapshot: LazySnapshotRef,
        mutations: &'a [super::WorkspaceMutation], key: acyclic_fs::IdempotencyKey,
        permit: acyclic_fs::PublicationPermit,
        io: &'a super::fork_preparer::PreparationIo,
        lease: &'a Arc<dyn crate::resource_preparation::CreatedTaskLease>)
        -> Result<crate::store::history::HistoryContentFuture<'a, RootLazyPublication<A, O>>>;
}

struct RetainedRootLazy<A, O, D, S> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: Option<acyclic_fs::NativeAllocation>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static,
    D: DemandSource + 'static, S: LazyWorkspaceStore> RootLazyBacking<A, O> for RetainedRootLazy<A, O, D, S>
{
    fn workspace_id(&self) -> acyclic_fs::WorkspaceId { self.lazy.workspace().id() }
    #[cfg(not(target_arch = "wasm32"))]
    fn matches_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool {
        self._custody.as_ref().is_some_and(|custody| custody.matches_work(work))
    }
    fn validate<'a>(&'a self, snapshot: LazySnapshotRef, budget: WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken)
        -> acyclic_stream::BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>>
    {
        Box::pin(async move {
            self.lazy.validate_snapshot_measured(snapshot, budget, cancellation).await
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, root_source_error))
        })
    }
    fn read<'a>(&'a self, snapshot: LazySnapshotRef, path: &'a str, maximum: u64,
        budget: WorkBudget, cancellation: &'a acyclic_fs::CancellationToken)
        -> acyclic_stream::BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_stream::OwnedBytes>, Error>>
    {
        Box::pin(async move {
            self.lazy.read_at_snapshot_measured(snapshot, path, maximum, budget, cancellation).await
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, root_source_error))
        })
    }
    fn list<'a>(&'a self, snapshot: LazySnapshotRef, path: &'a str,
        cursor: Option<acyclic_fs::LazyDirectoryCursor>, maximum: u32,
        budget: WorkBudget, cancellation: &'a acyclic_fs::CancellationToken)
        -> acyclic_stream::BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_fs::LazyDirectoryPage>, Error>>
    {
        Box::pin(async move {
            self.lazy.list_directory_at_snapshot_measured(snapshot, path, cursor, maximum, budget, cancellation).await
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, root_source_error))
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn validate_native<'a>(&'a self, snapshot: LazySnapshotRef,
        work: &'a acyclic_fs::NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            self.lazy.validate_snapshot_with_native_work(snapshot, work).await
                .map_err(Error::from)
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn read_native<'a>(&'a self, snapshot: LazySnapshotRef, path: &'a str, maximum: u64,
        work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>
    {
        crate::NativeFutureBundle::new(work, || async move {
            self.lazy.read_at_snapshot_with_work(snapshot, path, maximum, work, work.cancellation()).await
                .map_err(|(source, custody)| Error::NativeSource { source, custody })
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn publish<'a>(&'a self, snapshot: LazySnapshotRef,
        mutations: &'a [super::WorkspaceMutation], key: acyclic_fs::IdempotencyKey,
        permit: acyclic_fs::PublicationPermit,
        io: &'a super::fork_preparer::PreparationIo,
        lease: &'a Arc<dyn crate::resource_preparation::CreatedTaskLease>)
        -> Result<crate::store::history::HistoryContentFuture<'a, RootLazyPublication<A, O>>>
    {
        let generation = |commit| match commit {
            acyclic_fs::TransactionCommit::Committed(generation)
            | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => generation,
        };
        if let Some(original) = io.original_native_work() {
            return crate::NativeFutureBundle::new(original, || async move {
                let lease = Arc::clone(lease);
                let mut before_mutation = move |work: &acyclic_fs::NativeMaterializationWork| {
                    let original = acyclic_fs::NativeMaterializationWork::clone(work);
                    let lease = Arc::clone(&lease);
                    async move { lease.verify_with_native_work(&original)?.await }
                };
                let (commit, snapshot) = self.lazy.write_batch_at_snapshot_with_native_work_guarded(
                    snapshot, root_lazy_mutations(mutations), key, permit, original,
                    &io.cancellation, &mut before_mutation,
                ).await?;
                Ok((generation(commit), snapshot))
            }).map(crate::store::history::HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            let allowance = io.remaining()?;
            let (commit, snapshot) = if allowance.has_finite_allowances() {
                let mut before_mutation = |budget| async move {
                    let callback = super::fork_preparer::PreparationIo::from_budget(budget, &io.cancellation)
                        .map_err(acyclic_fs::OperationFailure::before_work)?;
                    let verified = callback.verify_publication_lease(lease.as_ref()).await;
                    match verified {
                        Ok(()) => Ok(acyclic_fs::OperationReceipt { value: (), work: callback.receipt() }),
                        Err(error) => Err(acyclic_fs::OperationFailure::new(error, callback.receipt())),
                    }
                };
                io.receive_content(self.lazy.write_batch_at_snapshot_measured_guarded(
                    snapshot, root_lazy_mutations(mutations), key, permit,
                    allowance, &io.cancellation, &mut before_mutation,
                ).await)?
            } else {
                let mut before_mutation = || async move { lease.verify().await };
                self.lazy.write_batch_at_snapshot_guarded(
                    snapshot, root_lazy_mutations(mutations), key, permit,
                    &io.cancellation, &mut before_mutation,
                ).await?
            };
            Ok((generation(commit), snapshot))
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FsRootNativeSource<A, O> {

    /// Funds the concrete adapter before allocation on the original ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_lazy_with_native_work<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        lazy: Arc<LazyWorkspace<A, O, D, S>>, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let custody = work.claim_allocation(std::mem::size_of::<RetainedRootLazy<A, O, D, S>>() as u64, 0)
            .map_err(Error::from)?;
        work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED })
            .map_err(Error::from)?;
        Ok(Self { backing: Arc::new(RetainedRootLazy { lazy, _custody: Some(custody) }) })
    }

    pub(crate) fn workspace_id(&self) -> acyclic_fs::WorkspaceId { self.backing.workspace_id() }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn matches_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool {
        self.backing.matches_native_work(work)
    }
    pub(crate) fn validate_snapshot_measured<'a>(&'a self, snapshot: LazySnapshotRef,
        budget: WorkBudget, cancellation: &'a acyclic_fs::CancellationToken)
        -> acyclic_stream::BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<()>, Error>>
    { self.backing.validate(snapshot, budget, cancellation) }
    pub(crate) fn read_measured<'a>(&'a self, snapshot: LazySnapshotRef, path: &'a str, maximum: u64,
        budget: WorkBudget, cancellation: &'a acyclic_fs::CancellationToken)
        -> acyclic_stream::BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_stream::OwnedBytes>, Error>>
    { self.backing.read(snapshot, path, maximum, budget, cancellation) }
    pub(crate) fn list_measured<'a>(&'a self, snapshot: LazySnapshotRef, path: &'a str,
        cursor: Option<acyclic_fs::LazyDirectoryCursor>, maximum: u32,
        budget: WorkBudget, cancellation: &'a acyclic_fs::CancellationToken)
        -> acyclic_stream::BoxProviderFuture<'a, acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<acyclic_fs::LazyDirectoryPage>, Error>>
    { self.backing.list(snapshot, path, cursor, maximum, budget, cancellation) }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn validate_snapshot_with_native_work<'a>(&'a self, snapshot: LazySnapshotRef,
        work: &'a acyclic_fs::NativeMaterializationWork) -> Result<crate::NativeFutureBundle<'a, ()>>
    { self.backing.validate_native(snapshot, work) }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn read_with_native_work<'a>(&'a self, snapshot: LazySnapshotRef, path: &'a str,
        maximum: u64, work: &'a acyclic_fs::NativeMaterializationWork)
        -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>
    { self.backing.read_native(snapshot, path, maximum, work) }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn publish_with_io<'a>(&'a self, snapshot: LazySnapshotRef,
        mutations: &'a [super::WorkspaceMutation], key: acyclic_fs::IdempotencyKey,
        permit: acyclic_fs::PublicationPermit,
        io: &'a super::fork_preparer::PreparationIo,
        lease: &'a Arc<dyn crate::resource_preparation::CreatedTaskLease>)
        -> Result<crate::store::history::HistoryContentFuture<'a, RootLazyPublication<A, O>>>
    {
        if mutations.iter().any(|mutation| matches!(mutation, super::WorkspaceMutation::Remove { .. })) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "Root upload candidate cannot contain a removal" });
        }
        if io.original_native_work().is_some_and(|work| !self.matches_native_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "Root upload candidate belongs to another original native ledger" });
        }
        self.backing.publish(snapshot, mutations, key, permit, io, lease)
    }
}

/// Creator-only project selection within one explicitly configured service graph.
pub struct FsRootProjectSourceProvider<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    owner: Authority,
    graph_root: TaskId,
    native: Option<(VolumeRef, FsRootNativeSource<A, O>)>,
    bootstrap: Option<Arc<super::fork_preparer::FilesystemRootBootstrapAllocation<A, O>>>,
    #[cfg(not(target_arch = "wasm32"))]
    _native_custody: Option<acyclic_fs::NativeAllocation>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FsRootProjectSourceProvider<A, O> {
    /// Requires the actual service owner and exact configured graph root.
    pub fn new(host: Arc<FilesystemHost<A, O>>, owner: Authority, graph_root: TaskId) -> Result<Self> {
        host.provider.validate()?;
        Ok(Self {
            host, owner, graph_root, native: None,
            #[cfg(not(target_arch = "wasm32"))]
            _native_custody: None,
        })
    }

    /// Copies the configured service owner on this original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        host: Arc<FilesystemHost<A, O>>, owner: &Authority, graph_root: TaskId,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        use crate::store::history::HistoryIo as _;
        host.provider.validate_with_native_work(work)?;
        let mut custody = work.retain_owned(std::mem::size_of::<Self>() as u64)?;
        let mut owner_custody;
        let mut io = crate::store::history::native_work_io(work)
            .map_err(Error::from)?;
        let owner = io.clone_authority(owner)?;
        owner_custody = io.into_custody();
        custody.absorb(&mut owner_custody).map_err(|cause| Error::NativeWork {
            cause, custody: Some(owner_custody),
        })?;
        Ok(Self {
            host, owner, graph_root, native: None, _native_custody: Some(custody),
        })
    }

    /// Shares the paid policy; stdlib Arc backing is not SDK payload.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn into_native_shared(self, work: &acyclic_fs::NativeMaterializationWork) -> Result<Arc<Self>> {
        if !self._native_custody.as_ref().is_some_and(|custody| custody.matches_work(work)) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "Root source policy belongs to another native ledger" });
        }
        work.charge(WorkCounters { allocation_operations: 1, ..WorkCounters::UNCHARGED })
            .map_err(Error::from)?;
        Ok(Arc::new(self))
    }

    /// Transfers only an already admitted Arc handle; all original record and Project payload remains guarded by the authentic creator token.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn with_bootstrap_source_with_native_work(
        mut self, proof: Arc<super::fork_preparer::FilesystemRootBootstrapAllocation<A, O>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        work.remaining()?;
        if proof.owner != self.owner || proof.root != self.graph_root
            || proof.request.volume.class() != VolumeClass::Project || !Arc::ptr_eq(&proof.host, &self.host)
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "Root bootstrap proof differs from this configured service" });
        }
        if !self._native_custody.as_ref().is_some_and(|custody| custody.matches_work(work))
            || !proof.matches_native_work(work)
        {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid,
                reason: "Root bootstrap proof requires its original configured native ledger" });
        }
        self.bootstrap = Some(proof);
        Ok(self)
    }

    fn native_source(&self, project: &VolumeRef) -> Option<&FsRootNativeSource<A, O>> {
        self.native.as_ref().filter(|(volume, _)| volume == project).map(|(_, source)| source)
            .or_else(|| self.bootstrap.as_ref().filter(|proof| &proof.request.volume == project)
                .and_then(|proof| proof.native_source.as_ref()))
    }

}

impl<A, O> RootProjectSourceProvider for FsRootProjectSourceProvider<A, O>
where
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    fn verify<'a>(&'a self, context: RootProjectSourceContext<'a>, snapshot: &'a ProjectSourceSnapshot)
        -> acyclic_stream::BoxProviderFuture<'a, Result<()>>
    {
        Box::pin(async move {
            if context.owner != &self.owner || context.graph_root != self.graph_root
                || !context.created_project_authenticated
            {
                return Err(Error::Unauthorized("project source lacks this graph's authenticated creator".into()));
            }
            if self.native_source(&snapshot.project).is_some()
                && snapshot.native_snapshot.is_none()
            {
                return Err(Error::Unauthorized("bound lazy project requires its original native snapshot".into()));
            }
            snapshot.project.validate()?;
            snapshot.generation.validate()?;
            if snapshot.project.class() != VolumeClass::Project
                || snapshot.project.provider() != &self.host.provider
                || snapshot.generation.as_resource().provider() != &self.host.provider
            {
                return Err(Error::Unauthorized("project source belongs to another namespace or provider".into()));
            }
            let mut admission = SourceAdmission::new(context.maximum_work, context.maximum_bytes, context.cancellation)?;
            let bounds = JsonBounds { maximum_bytes: context.maximum_bytes, maximum_items: context.maximum_work, maximum_depth: 32 };
            let encoded = canonical_json_bytes_bounded(&snapshot.project, bounds, &mut |charge| admission.codec(charge))?;
            let encoded_owned = encoded.len() as u64;
            admission.charge(WorkCounters { bytes_hashed: encoded_owned, ..WorkCounters::default() })?;
            let digest = blake3::hash(&encoded).to_hex();
            drop(encoded);
            admission.release(encoded_owned)?;
            let mut name = [0_u8; 72];
            name[..8].copy_from_slice(b"harness-");
            name[8..].copy_from_slice(digest.as_bytes());
            let mut read = [0_u8; 76];
            read[..12].copy_from_slice(b"volume:read:");
            read[12..].copy_from_slice(digest.as_bytes());
            let name = std::str::from_utf8(&name).map_err(|_| Error::Invalid("invalid source namespace digest".into()))?;
            let read = std::str::from_utf8(&read).map_err(|_| Error::Invalid("invalid source read digest".into()))?;
            if !context.source.grants.contains(read) {
                return Err(Error::Unauthorized("selected source admission lacks original project read authority".into()));
            }
            let opened = self.host.filesystem.open_workspace_measured(name, admission.remaining()?, context.cancellation).await
                .map_err(|failure| map_error(failure.error))?;
            admission.merge(opened.work)?;
            admission.retain(opened.value.name().as_str().len() as u64)?;
            let generation_bytes: [u8; 32] = snapshot.generation.as_resource().key().try_into()
                .map_err(|_| Error::Invalid("project generation key must be 32 bytes".into()))?;
            let generation_id = GenerationId::new(Digest::from_bytes(generation_bytes));
            let selected = opened.value.generation_measured(generation_id, admission.remaining()?, context.cancellation).await
                .map_err(|failure| map_error(failure.error))?;
            admission.merge(selected.work)?;
            drop(selected.value);
            if let Some(value) = &snapshot.native_snapshot {
                let Some(lazy) = self.native_source(&snapshot.project) else {
                    return Err(Error::Unauthorized("native source has no original provider binding".into()));
                };
                if lazy.workspace_id() != opened.value.id() {
                    return Err(Error::Unauthorized("native source names another original workspace".into()));
                }
                let encoded = canonical_json_bytes_bounded(value, bounds, &mut |charge| admission.codec(charge))?;
                let owned = encoded.len() as u64;
                let frozen = LazySnapshotRef::deserialize(value)
                    .map_err(|_| Error::Invalid("native source is not a lazy snapshot reference".into()))?;
                drop(encoded);
                admission.release(owned)?;
                if frozen.workspace_id != opened.value.id() || frozen.authored_generation != generation_id {
                    return Err(Error::Unauthorized("native capture does not match the selected generation".into()));
                }
                let verified = lazy.validate_snapshot_measured(frozen, admission.remaining()?, context.cancellation).await
                    .map_err(|failure| failure.error)?;
                admission.merge(verified.work)?;
            }
            context.cancellation.check().map_err(|_| Error::Unauthorized("project source operation cancelled".into()))?;
            Ok(())
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self, context: RootProjectSourceContext<'a>,
        snapshot: &'a ProjectSourceSnapshot,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        context.cancellation.check().map_err(|_| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None,
        })?;
        crate::NativeFutureBundle::new(work, || async move {
            work.remaining()?;
            context.cancellation.check().map_err(|_| Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None,
            })?;
            if context.owner != &self.owner || context.graph_root != self.graph_root
                || !context.created_project_authenticated
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source lacks this graph's authenticated creator",
                });
            }
            if context.maximum_work == 0 || context.maximum_work == u64::MAX
                || context.maximum_bytes == 0 || context.maximum_bytes == u64::MAX
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "project source requires finite positive admission",
                });
            }
            if snapshot.project.class() != VolumeClass::Project
                || snapshot.project.provider() != &self.host.provider
                || snapshot.generation.as_resource().provider() != &self.host.provider
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "project source belongs to another namespace or provider",
                });
            }
            if self.native_source(&snapshot.project).is_some()
                && snapshot.native_snapshot.is_none()
            {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "bound lazy project requires its original native snapshot",
                });
            }
            let io = super::fork_preparer::PreparationIo::from_native_work(work)?;
            let bounds = JsonBounds {
                maximum_bytes: context.maximum_bytes,
                maximum_items: context.maximum_work, maximum_depth: 32,
            };
            let encoded = native_source_json(&snapshot.project, bounds, &io, work)?;
            let hashing = WorkCounters {
                bytes_hashed: encoded.len() as u64, ..WorkCounters::default()
            };
            work.admit(hashing)?;
            let digest = blake3::hash(&encoded);
            work.spent(hashing)?;
            drop(encoded);
            let copying = WorkCounters {
                bytes_encoded: 64, bytes_copied: 148, ..WorkCounters::default()
            };
            work.admit(copying)?;
            let digest = digest.to_hex();
            let mut name = [0_u8; 72];
            name[..8].copy_from_slice(b"harness-");
            name[8..].copy_from_slice(digest.as_bytes());
            let mut read = [0_u8; 76];
            read[..12].copy_from_slice(b"volume:read:");
            read[12..].copy_from_slice(digest.as_bytes());
            work.spent(copying)?;
            let name = std::str::from_utf8(&name).map_err(|_| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "invalid source namespace digest",
            })?;
            let read = std::str::from_utf8(&read).map_err(|_| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "invalid source read digest",
            })?;
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
            if !context.source.grants.contains(read) {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "selected source admission lacks original project read authority",
                });
            }
            let opened = self.host.filesystem.open_workspace_with_native_work(
                name, work, context.cancellation,
            ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
            let selected = super::fork_preparer::bounded_io::generation(
                &self.host, &opened, &snapshot.generation, &io,
            ).await?;
            let generation_id = selected.id();
            drop(selected);
            if let Some(value) = &snapshot.native_snapshot {
                let Some(lazy) = self.native_source(&snapshot.project) else {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "native source has no original provider binding",
                    });
                };
                if lazy.workspace_id() != opened.id() {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "native source names another original workspace",
                    });
                }
                let encoded = native_source_json(value, bounds, &io, work)?;
                let _frozen_hold = work.retain_owned(std::mem::size_of::<LazySnapshotRef>() as u64)?;
                let frozen: LazySnapshotRef = crate::contract::json_from_slice_bounded_native_trusted(
                    &encoded, bounds, 0, crate::wire::ErrorCode::Invalid,
                    crate::contract::JsonValueSchema::None,
                    &mut |charge| io.codec_charge(charge, false),
                )?;
                drop(encoded);
                if frozen.workspace_id != opened.id() || frozen.authored_generation != generation_id {
                    return Err(Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Unauthorized,
                        reason: "native capture does not match the selected generation",
                    });
                }
                lazy.validate_snapshot_with_native_work(frozen, work)?.await?;
            }
            context.cancellation.check().map_err(|_| Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None,
            })?;
            work.remaining()?;
            Ok(())
        })
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn native_source_json<T: serde::Serialize>(
    value: &T, bounds: JsonBounds, io: &super::fork_preparer::PreparationIo,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<acyclic_stream::OwnedBytes> {
    let diagnostic = work.retain_owned(crate::contract::sdk_json_diagnostic_bytes())?;
    let bytes = crate::contract::canonical_json_bytes_bounded_native_trusted(
        value, bounds, crate::wire::ErrorCode::Invalid,
        &mut |charge| io.codec_charge(charge, true),
    ).map_err(|error| match error {
        Error::NativeEncode { code, source, custody: None } =>
            Error::NativeEncode { code, source, custody: Some(diagnostic) },
        error => error,
    })?;
    super::fork_preparer::bounded_io::source_bytes_from_vec(bytes, Some(io))
}

struct SourceAdmission<'a> {
    work: WorkCounters,
    budget: WorkBudget,
    live: u64,
    cancellation: &'a acyclic_fs::CancellationToken,
}

impl<'a> SourceAdmission<'a> {
    fn new(maximum_work: u64, maximum_bytes: u64, cancellation: &'a acyclic_fs::CancellationToken) -> Result<Self> {
        if maximum_work == 0 || maximum_work == u64::MAX || maximum_bytes == 0 || maximum_bytes == u64::MAX {
            return Err(Error::Invalid("project source requires finite positive admission".into()));
        }
        cancellation.check().map_err(source_cancelled)?;
        let operations = maximum_work / 14;
        let bytes = maximum_bytes / 9;
        Ok(Self {
            work: WorkCounters::default(), live: 0, cancellation,
            budget: WorkCounters {
                authority_records_read: operations, authority_records_appended: operations,
                authority_bytes_read: bytes, authority_bytes_written: bytes,
                object_probes: operations, backend_read_operations: operations, backend_write_operations: operations,
                durability_operations: operations, page_reads: operations, page_writes: operations,
                object_bytes_read: bytes, object_bytes_written: bytes,
                bytes_hashed: bytes, bytes_copied: bytes, bytes_encoded: bytes,
                source_bytes_read: bytes, source_path_components: operations, source_entries_visited: operations,
                output_bytes: bytes, items_examined: operations, items_returned: operations,
                allocation_operations: operations, peak_allocation_bytes: maximum_bytes, materializations: operations,
            },
        })
    }

    fn remaining(&self) -> Result<WorkBudget> {
        self.cancellation.check().map_err(source_cancelled)?;
        let mut remaining = self.work.remaining(self.budget).map_err(quota_error)?;
        remaining.peak_allocation_bytes = self.budget.peak_allocation_bytes.checked_sub(self.live)
            .ok_or_else(|| Error::Invalid("project source storage accounting overflow".into()))?;
        Ok(remaining)
    }

    fn charge(&mut self, delta: WorkCounters) -> Result<()> {
        self.cancellation.check().map_err(source_cancelled)?;
        self.work.charge(&delta, &self.budget).map_err(quota_error)
    }

    fn merge(&mut self, child: WorkCounters) -> Result<()> {
        let overlap = self.live.checked_add(child.peak_allocation_bytes)
            .ok_or_else(|| Error::Invalid("project source storage accounting overflow".into()))?;
        let mut combined = self.work.checked_add(child).map_err(quota_error)?;
        combined.peak_allocation_bytes = combined.peak_allocation_bytes.max(overlap);
        WorkCounters::default().admit(&combined, &self.budget).map_err(quota_error)?;
        self.work = combined;
        Ok(())
    }

    fn retain(&mut self, bytes: u64) -> Result<()> {
        self.cancellation.check().map_err(source_cancelled)?;
        let live = self.live.checked_add(bytes).ok_or_else(|| Error::Invalid("project source storage accounting overflow".into()))?;
        self.work.charge_allocation(0, live, &self.budget).map_err(quota_error)?;
        self.live = live;
        Ok(())
    }

    fn release(&mut self, bytes: u64) -> Result<()> {
        self.live = self.live.checked_sub(bytes).ok_or_else(|| Error::Invalid("project source storage release mismatch".into()))?;
        Ok(())
    }

    fn codec(&mut self, charge: JsonCodecCharge) -> Result<()> {
        match charge {
            JsonCodecCharge::Work { bytes, items } => self.charge(WorkCounters { bytes_encoded: bytes, items_examined: items, ..WorkCounters::default() }),
            JsonCodecCharge::Copy { bytes } => self.charge(WorkCounters { bytes_copied: bytes, ..WorkCounters::default() }),
            JsonCodecCharge::Read { bytes } => self.charge(WorkCounters { source_bytes_read: bytes, ..WorkCounters::default() }),
            JsonCodecCharge::Retain { bytes } => self.retain(bytes),
            JsonCodecCharge::Release { bytes } => self.release(bytes),
            JsonCodecCharge::CopyAdmission { bytes } => {
                self.remaining()?;
                self.work.admit(&WorkCounters { bytes_copied: bytes, ..WorkCounters::default() },
                    &self.budget).map_err(quota_error)
            }
            JsonCodecCharge::AllocationOperation { bytes } =>
                self.charge(WorkCounters { allocation_operations: u64::from(bytes != 0),
                    ..WorkCounters::default() }),
            JsonCodecCharge::Inspect { items } =>
                self.charge(WorkCounters { items_examined: items, ..WorkCounters::default() }),
            JsonCodecCharge::Admission { maximum_owned_bytes, .. } => {
                let available = self.remaining()?;
                WorkCounters::default().admit(&WorkCounters {
                    peak_allocation_bytes: maximum_owned_bytes, ..WorkCounters::default()
                }, &available).map_err(quota_error)
            }
        }
    }
}

fn quota_error(error: acyclic_fs::WorkError) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    { Error::NativeFilesystem { source: acyclic_fs::FsError::Work(error), custody: None } }
    #[cfg(target_arch = "wasm32")]
    { Error::Invalid(format!("project source quota: {error}")) }
}
fn source_cancelled(error: acyclic_fs::CancellationError) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    { Error::NativeFilesystem { source: acyclic_fs::FsError::Cancelled(error), custody: None } }
    #[cfg(target_arch = "wasm32")]
    { let _ = error; Error::Unauthorized("project source operation cancelled".into()) }
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use super::*;
    use crate::conversation::{VolumeOperation, VolumeOwner};
    use crate::core::{AggregateKind, Limits};
    use crate::runtime::{TaskAdmissionRecord, TaskRunLimits};

    #[test]
    fn source_codec_copy_preflight_is_not_spent_or_fabricated_storage() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let mut source = SourceAdmission::new(1_400, 900, &cancellation)?;
        source.codec(JsonCodecCharge::CopyAdmission { bytes: 8 })?;
        assert_eq!(source.work.bytes_copied, 0);
        assert_eq!(source.live, 0);
        source.codec(JsonCodecCharge::Retain { bytes: 8 })?;
        source.codec(JsonCodecCharge::AllocationOperation { bytes: 8 })?;
        source.codec(JsonCodecCharge::Copy { bytes: 8 })?;
        source.codec(JsonCodecCharge::Inspect { items: 3 })?;
        assert_eq!(source.work.bytes_copied, 8);
        assert_eq!(source.work.allocation_operations, 1);
        assert_eq!(source.work.items_examined, 3);
        assert_eq!(source.live, 8);
        let prefix = source.work;
        assert!(matches!(source.codec(JsonCodecCharge::CopyAdmission { bytes: 101 }),
            Err(Error::NativeFilesystem { source: acyclic_fs::FsError::Work(_), .. })));
        assert_eq!(source.work, prefix);
        cancellation.cancel();
        source.codec(JsonCodecCharge::Release { bytes: 8 })?;
        assert_eq!(source.live, 0);
        assert_eq!(source.work, prefix);
        Ok(())
    }

    fn admission(root: crate::OperationId, project: &VolumeRef) -> Result<TaskAdmissionRecord> {
        Ok(TaskAdmissionRecord {
            operation_id: root,
            task: crate::registry::ComponentIdentity {
                name: "test.root-bootstrap".into(), version: "1".into(), digest: [41; 32],
            },
            machine: crate::workflow::MachineIdentity {
                name: "test.root-bootstrap".into(), version: "1".into(), digest: [42; 32],
            },
            input: serde_json::json!({}), input_schema: serde_json::json!({"type":"object"}),
            output_schema: serde_json::json!({"type":"object"}), parent: None,
            resource_proofs: Vec::new(),
            grants: crate::Capabilities::new([project.capability(VolumeOperation::Read)?]),
            limits: Limits::default(), run_limits: TaskRunLimits::default(),
            policy: None, extensions: None, execution: None,
        })
    }

    fn context<'a>(owner: &'a Authority, admission: &'a TaskAdmissionRecord,
        root: TaskId, cancellation: &'a acyclic_fs::CancellationToken) -> RootProjectSourceContext<'a> {
        RootProjectSourceContext {
            owner, caller: admission, source: admission, graph_root: root,
            operation_id: crate::OperationId::from_bytes([45; 16]),
            maximum_work: 14_000_000, maximum_bytes: 90_000_000,
            cancellation, created_project_authenticated: false,
        }
    }

    #[tokio::test]
    async fn real_filesystem_birth_without_protected_root_association_is_not_source_authority() -> Result<()> {
        let provider = crate::resources::ProviderRef::new("root-birth", "filesystem", "2")?;
        let host = Arc::new(FilesystemHost::new(acyclic_fs::Fs::memory(), provider.clone())?);
        let project = VolumeRef::new(provider, "root-project", VolumeClass::Project,
            VolumeOwner::Project("root-project".into()))?;
        let owner = Authority { kind: AggregateKind::Task, id: "root-owner".into() };
        let operation = acyclic_fs::OperationId::from_bytes([43; 16]);
        let root_operation = crate::OperationId::from_bytes([44; 16]);
        let root = crate::runtime::task_id_for_admission(root_operation);
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = SourceAdmission::new(14_000_000, 90_000_000, &cancellation)?.budget;
        let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
            .expect("finite original Root bootstrap ledger");
        let config = acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Durable);
        let birth = host.filesystem.create_workspace_with_operation_with_native_work(
            &project.storage_name()?, config, operation, &work, &cancellation,
        ).await.map_err(|(source, custody)| Error::NativeWorkspace { source, custody })?;
        let selected = ProjectSourceSnapshot {
            project: project.clone(), generation: host.generation_ref(&birth)?, native_snapshot: None,
        };
        drop(birth);
        let policy = FsRootProjectSourceProvider::new_with_native_work(host, &owner, root, &work)?;
        let original = admission(root_operation, &project)?;
        let before = work.receipt().backend_read_operations;
        assert!(matches!(policy.verify_with_native_work(
            context(&owner, &original, root, &cancellation), &selected, &work)?.await,
            Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, .. })));
        assert_eq!(work.receipt().backend_read_operations, before);
        Ok(())
    }
}
