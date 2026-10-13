use super::*;
use crate::conversation::BoundedContentFuture;
use crate::filesystem::{PreparationIo, WorkspaceMutation};
use acyclic_fs::facade::FsWork;
use acyclic_fs::kernel::KernelOwnedAllocation;

/// A retained original lazy source.
pub struct FsRootNativeSource<A, O> {
    pub(crate) backing: Arc<dyn RootLazyBacking<A, O>>,
}
impl<A, O> Clone for FsRootNativeSource<A, O> {
    fn clone(&self) -> Self {
        Self {
            backing: Arc::clone(&self.backing),
        }
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn root_lazy_mutations(
    mutations: &[crate::filesystem::WorkspaceMutation],
) -> impl ExactSizeIterator<Item = (&str, Option<acyclic_stream::OwnedBytes>)> + Clone + Send {
    mutations.iter().map(|mutation| match mutation {
        crate::filesystem::WorkspaceMutation::CreateDirectory { path } => (path.as_str(), None),
        crate::filesystem::WorkspaceMutation::PutFile { path, bytes } => {
            (path.as_str(), Some(bytes.clone()))
        }
        crate::filesystem::WorkspaceMutation::Remove { .. } => {
            unreachable!("root upload rejects removals before construction")
        }
    })
}

#[cfg(not(target_arch = "wasm32"))]
#[derive(Clone)]
struct RootPublicationGuard {
    lease: Arc<dyn crate::resource_preparation::CreatedTaskLease>,
}

#[cfg(not(target_arch = "wasm32"))]
impl acyclic_fs::AuthorityGuard for RootPublicationGuard {
    type Error = Error;
    async fn seal(
        &mut self,
        work: &mut acyclic_fs::AuthorityWork<'_>,
    ) -> std::result::Result<(), acyclic_fs::AuthorityError<Error>> {
        let io = match work.original {
            Some(original) => crate::filesystem::PreparationIo::from_native_work(original),
            None => {
                crate::filesystem::PreparationIo::from_budget(work.remaining()?, work.cancellation)
            }
        }
        .map_err(acyclic_fs::AuthorityError::Owner)?;
        let result = match self.lease.verify(Some(&io)) {
            Ok(verification) => verification.await,
            Err(error) => Err(error),
        };
        if work.original.is_none() {
            work.work = work
                .work
                .checked_add(io.receipt())
                .map_err(|error| acyclic_fs::AuthorityError::Authority(error.into()))?;
        }
        result.map_err(acyclic_fs::AuthorityError::Owner)
    }
}

pub(crate) trait RootLazyBacking<A, O>: acyclic_stream::ProviderPlatform {
    fn workspace_id(&self) -> acyclic_fs::WorkspaceId;
    #[cfg(not(target_arch = "wasm32"))]
    fn matches_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool;
    fn validate<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        io: &'a PreparationIo,
    ) -> Result<BoundedContentFuture<'a, ()>>;
    fn read<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        maximum: u64,
        io: &'a PreparationIo,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>;
    fn list<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        cursor: Option<acyclic_fs::LazyDirectoryCursor>,
        maximum: u32,
        io: &'a PreparationIo,
    ) -> Result<BoundedContentFuture<'a, (acyclic_fs::LazyDirectoryPage, KernelOwnedAllocation)>>;
    #[cfg(not(target_arch = "wasm32"))]
    fn publish<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        mutations: &'a [WorkspaceMutation],
        key: acyclic_fs::IdempotencyKey,
        permit: acyclic_fs::PublicationPermit,
        io: &'a PreparationIo,
        lease: &'a Arc<dyn crate::resource_preparation::CreatedTaskLease>,
    ) -> Result<BoundedContentFuture<'a, (acyclic_fs::Generation<A, O>, Result<LazySnapshotRef>)>>;
}

struct RetainedRootLazy<A, O, D, S> {
    lazy: Arc<LazyWorkspace<A, O, D, S>>,
    #[cfg(not(target_arch = "wasm32"))]
    _custody: acyclic_fs::NativeAllocation,
}
impl<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
> RootLazyBacking<A, O> for RetainedRootLazy<A, O, D, S>
where
    for<'source> &'source LazyWorkspace<A, O, D, S>: Send,
{
    fn workspace_id(&self) -> acyclic_fs::WorkspaceId {
        self.lazy.workspace().id()
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn matches_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool {
        self._custody.work().shares_allocation_context(work)
    }
    fn validate<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        io: &'a PreparationIo,
    ) -> Result<BoundedContentFuture<'a, ()>> {
        BoundedContentFuture::new(Some(io), || async move {
            let mut accounting = (&io).accounting(io.cancellation());
            let result = self
                .lazy
                .validate_snapshot(snapshot, &mut accounting)
                .await
                .map_err(Error::from);
            io.sdk((&io).finish(result, &accounting))
        })
    }
    fn read<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        maximum: u64,
        io: &'a PreparationIo,
    ) -> Result<BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        BoundedContentFuture::new(Some(io), || async move {
            let mut accounting = (&io).accounting(io.cancellation());
            let result = self
                .lazy
                .read_at_snapshot(snapshot, path, None, maximum, &mut accounting)
                .await
                .map_err(Error::from);
            let (chunk, _) = io.sdk((&io).finish(result, &accounting))?;
            if !chunk.is_intrinsic() {
                io.retain_owned(chunk.logical_owned_bytes())?;
            }
            Ok(chunk.bytes)
        })
    }
    fn list<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        cursor: Option<acyclic_fs::LazyDirectoryCursor>,
        maximum: u32,
        io: &'a PreparationIo,
    ) -> Result<BoundedContentFuture<'a, (acyclic_fs::LazyDirectoryPage, KernelOwnedAllocation)>>
    {
        BoundedContentFuture::new(Some(io), || async move {
            let mut accounting = (&io).accounting(io.cancellation());
            let result = self
                .lazy
                .list_directory_at_snapshot(snapshot, path, cursor, maximum, &mut accounting)
                .await
                .map(|(page, _, guard)| (page, guard))
                .map_err(Error::from);
            io.sdk((&io).finish(result, &accounting))
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn publish<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        mutations: &'a [WorkspaceMutation],
        key: acyclic_fs::IdempotencyKey,
        permit: acyclic_fs::PublicationPermit,
        io: &'a PreparationIo,
        lease: &'a Arc<dyn crate::resource_preparation::CreatedTaskLease>,
    ) -> Result<BoundedContentFuture<'a, (acyclic_fs::Generation<A, O>, Result<LazySnapshotRef>)>>
    {
        if mutations
            .iter()
            .any(|mutation| matches!(mutation, WorkspaceMutation::Remove { .. }))
        {
            return Err(io.invalid("Root upload candidate cannot contain a removal"));
        }
        if io
            .original_native_work()
            .is_some_and(|work| !self.matches_native_work(work))
        {
            return Err(
                io.invalid("Root upload candidate belongs to another original native ledger")
            );
        }
        BoundedContentFuture::new(Some(io), || async move {
            let mut guard = RootPublicationGuard {
                lease: Arc::clone(lease),
            };
            let mut accounting = (&io).accounting(io.cancellation());
            let result = self
                .lazy
                .write_at_snapshot(
                    snapshot,
                    root_lazy_mutations(mutations),
                    key,
                    permit,
                    &mut accounting,
                    &mut guard,
                )
                .await
                .map_err(Error::from);
            let (commit, snapshot) = io.sdk((&io).finish(result, &accounting))?;
            let generation = match commit {
                acyclic_fs::TransactionCommit::Committed(generation)
                | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => generation,
                acyclic_fs::TransactionCommit::Conflict { .. }
                | acyclic_fs::TransactionCommit::Fenced
                | acyclic_fs::TransactionCommit::IdempotencyConflict => {
                    return Err(crate::executor::stock_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Conflict,
                        "Root upload publication was rejected",
                    ));
                }
            };
            Ok((generation, snapshot))
        })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FsRootNativeSource<A, O> {
    /// Retains the concrete lazy source on its original ledger before sharing it.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_lazy<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        lazy: Arc<LazyWorkspace<A, O, D, S>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let custody = work.claim(
            std::mem::size_of::<RetainedRootLazy<A, O, D, S>>() as u64,
            0,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })?;
        Ok(Self {
            backing: Arc::new(RetainedRootLazy {
                lazy,
                _custody: custody,
            }),
        })
    }
}
