use super::*;
use acyclic_fs::facade::FsWork;
/// A retained original lazy source.
pub struct FsRootNativeSource<A, O> {
    pub(crate) backing: Arc<dyn RootLazyBacking<A, O>>,
}

impl<A, O> Clone for FsRootNativeSource<A, O> {
    fn clone(&self) -> Self {
        Self {
            backing: self.backing.clone(),
        }
    }
}

fn root_source_error(source: acyclic_fs::LazyWorkspaceError) -> Error {
    #[cfg(not(target_arch = "wasm32"))]
    {
        Error::from(source)
    }
    #[cfg(target_arch = "wasm32")]
    {
        Error::Storage(source.to_string())
    }
}
#[cfg(not(target_arch = "wasm32"))]
fn root_lazy_mutations(mutations: &[crate::filesystem::WorkspaceMutation])
    -> impl ExactSizeIterator<Item = (&str, Option<acyclic_stream::OwnedBytes>)> + Clone + Send {
    mutations.iter().map(|mutation| match mutation {
        crate::filesystem::WorkspaceMutation::CreateDirectory { path } => (path.as_str(), None),
        crate::filesystem::WorkspaceMutation::PutFile { path, bytes } => (path.as_str(), Some(bytes.clone())),
        crate::filesystem::WorkspaceMutation::Remove { .. } => unreachable!("root upload rejects removals before construction"),
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
    async fn seal(&mut self, work: &mut acyclic_fs::AuthorityWork<'_>) -> std::result::Result<(), acyclic_fs::AuthorityError<Error>> {
        if let Some(original) = work.original {
            let io = crate::filesystem::PreparationIo::from_native_work(original)
                .map_err(acyclic_fs::AuthorityError::Owner)?;
            return self.lease.verify(Some(&io))
                .map_err(acyclic_fs::AuthorityError::Owner)?.await.map_err(acyclic_fs::AuthorityError::Owner);
        }
        let callback = crate::filesystem::PreparationIo::from_budget(work.remaining()?, work.cancellation)
            .map_err(acyclic_fs::AuthorityError::Owner)?;
        let result = self.lease.verify(Some(&callback))
            .map_err(acyclic_fs::AuthorityError::Owner)?.await;
        work.work = work.work.checked_add(callback.receipt()).map_err(|error| acyclic_fs::AuthorityError::Authority(error.into()))?;
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
        io: &'a crate::filesystem::PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>>;
    fn read<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        maximum: u64,
        io: &'a crate::filesystem::PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>>;
    fn list<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        cursor: Option<acyclic_fs::LazyDirectoryCursor>,
        maximum: u32,
        budget: WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        acyclic_fs::MeasuredResult<
            acyclic_fs::OperationReceipt<acyclic_fs::LazyDirectoryPage>,
            Error,
        >,
    >;
    #[cfg(not(target_arch = "wasm32"))]
    fn publish<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        mutations: &'a [crate::filesystem::WorkspaceMutation],
        key: acyclic_fs::IdempotencyKey,
        permit: acyclic_fs::PublicationPermit,
        io: &'a crate::filesystem::PreparationIo,
        lease: &'a Arc<dyn crate::resource_preparation::CreatedTaskLease>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (acyclic_fs::Generation<A, O>, Result<LazySnapshotRef>),
        >,
    >;
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
        io: &'a crate::filesystem::PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        let validate = || async move {
            let mut accounting = acyclic_fs::facade::FsWork::accounting(&io, io.cancellation());
            let result = self.lazy.validate_snapshot(snapshot, &mut accounting);
            let (result, recorded) = acyclic_fs::facade::FsWork::finish(&io, result, &accounting);
            recorded?;
            result.map_err(root_source_error)
        };
        crate::conversation::BoundedContentFuture::new(Some(io), validate)
    }
    fn read<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        maximum: u64,
        io: &'a crate::filesystem::PreparationIo,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, acyclic_stream::OwnedBytes>> {
        let read = || async move {
            let mut accounting = acyclic_fs::facade::FsWork::accounting(&io, io.cancellation());
            let result = self.lazy.read_at_snapshot(snapshot, path, None, maximum, &mut accounting).await;
            let (result, recorded) = acyclic_fs::facade::FsWork::finish(&io, result, &accounting);
            recorded?;
            let (chunk, _) = result.map_err(root_source_error)?;
            if !chunk.is_intrinsic() {
                io.retain_owned(chunk.logical_owned_bytes())?;
            }
            Ok(chunk.bytes)
        };
        crate::conversation::BoundedContentFuture::new(Some(io), read)
    }
    fn list<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        cursor: Option<acyclic_fs::LazyDirectoryCursor>,
        maximum: u32,
        budget: WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> acyclic_stream::BoxProviderFuture<
        'a,
        acyclic_fs::MeasuredResult<
            acyclic_fs::OperationReceipt<acyclic_fs::LazyDirectoryPage>,
            Error,
        >,
    > {
        Box::pin(async move {
            let mut accounting = budget.accounting(cancellation);
            let result = self.lazy
                .list_directory_at_snapshot(
                    snapshot,
                    path,
                    cursor,
                    maximum,
                    &mut accounting,
                )
                .await
                .map(|(page, _, _)| page)
                .map_err(root_source_error);
            budget.finish(result, &accounting)
        })
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn publish<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        mutations: &'a [crate::filesystem::WorkspaceMutation],
        key: acyclic_fs::IdempotencyKey,
        permit: acyclic_fs::PublicationPermit,
        io: &'a crate::filesystem::fork_preparer::PreparationIo,
        lease: &'a Arc<dyn crate::resource_preparation::CreatedTaskLease>,
    ) -> Result<
        crate::conversation::BoundedContentFuture<
            'a,
            (acyclic_fs::Generation<A, O>, Result<LazySnapshotRef>),
        >,
    > {
        if mutations.iter().any(|mutation| {
            matches!(
                mutation,
                crate::filesystem::WorkspaceMutation::Remove { .. }
            )
        }) {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "Root upload candidate cannot contain a removal",
            });
        }
        if io
            .original_native_work()
            .is_some_and(|work| !self.matches_native_work(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "Root upload candidate belongs to another original native ledger",
            });
        }
        let generation = |commit| match commit {
            acyclic_fs::TransactionCommit::Committed(generation)
            | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
            acyclic_fs::TransactionCommit::Conflict { .. }
            | acyclic_fs::TransactionCommit::Fenced
            | acyclic_fs::TransactionCommit::IdempotencyConflict => {
                Err(crate::executor::stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "Root upload publication was rejected",
                ))
            }
        };
        let guard = RootPublicationGuard { lease: Arc::clone(lease) };
        let publish = || async move {
            let mut accounting = (&io).accounting(io.cancellation());
            let result = self.lazy.write_at_snapshot(
                snapshot, root_lazy_mutations(mutations), key, permit, &mut accounting, &mut guard.clone(),
            ).await;
            let (result, recorded) = (&io).finish(result, &accounting);
            recorded?;
            let (commit, snapshot) = result?;
            Ok((generation(commit)?, snapshot))
        };
        crate::conversation::BoundedContentFuture::new(Some(io), publish)
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FsRootNativeSource<A, O> {
    /// Retains the concrete lazy source on its original ledger before sharing it.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn from_lazy<D: DemandSource + 'static, S: LazyWorkspaceStore>(
        lazy: Arc<LazyWorkspace<A, O, D, S>>,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let custody = work
            .claim(
                std::mem::size_of::<RetainedRootLazy<A, O, D, S>>() as u64,
                0,
                acyclic_fs::WorkCounters::UNCHARGED,
            )
            .map_err(Error::from)?;
        work.charge(WorkCounters {
            allocation_operations: 1,
            ..WorkCounters::default()
        })
        .map_err(Error::from)?;
        Ok(Self {
            backing: Arc::new(RetainedRootLazy {
                lazy,
                _custody: custody,
            }),
        })
    }
}
