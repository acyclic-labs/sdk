use super::*;
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
fn root_lazy_mutations(
    mutations: &[crate::filesystem::WorkspaceMutation],
) -> impl Iterator<Item = acyclic_fs::lazy_workspace::LazyWriteMutation<'_>> + Send {
    mutations.iter().filter_map(|mutation| match mutation {
        crate::filesystem::WorkspaceMutation::CreateDirectory { path } => {
            Some(acyclic_fs::lazy_workspace::LazyWriteMutation::CreateDirectory { path })
        }
        crate::filesystem::WorkspaceMutation::PutFile { path, bytes } => {
            Some(acyclic_fs::lazy_workspace::LazyWriteMutation::PutFile {
                path,
                bytes: bytes.clone(),
            })
        }
        crate::filesystem::WorkspaceMutation::Remove { .. } => None,
    })
}

pub(crate) trait RootLazyBacking<A, O>: acyclic_stream::ProviderPlatform {
    fn workspace_id(&self) -> acyclic_fs::WorkspaceId;
    #[cfg(not(target_arch = "wasm32"))]
    fn matches_native_work(&self, work: &acyclic_fs::NativeMaterializationWork) -> bool;
    fn validate<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        io: &'a crate::filesystem::PreparationIo,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>>;
    fn read<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        maximum: u64,
        io: &'a crate::filesystem::PreparationIo,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, acyclic_stream::OwnedBytes>>;
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
        crate::store::history::HistoryContentFuture<
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
    ) -> Result<crate::store::history::HistoryContentFuture<'a, ()>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return crate::NativeFutureBundle::new(work, || async move {
                self.lazy
                    .validate_snapshot(snapshot, &mut root_accounting(io)?)
                    .map_err(Error::from)
            })
            .map(crate::store::history::HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            let mut accounting = root_accounting(io)?;
            let result = self.lazy.validate_snapshot(snapshot, &mut accounting);
            receive_root(result, accounting, io)
        })
    }
    fn read<'a>(
        &'a self,
        snapshot: LazySnapshotRef,
        path: &'a str,
        maximum: u64,
        io: &'a crate::filesystem::PreparationIo,
    ) -> Result<crate::store::history::HistoryContentFuture<'a, acyclic_stream::OwnedBytes>> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.original_native_work() {
            return crate::NativeFutureBundle::new(work, || async move {
                self.lazy
                    .read_at_snapshot(snapshot, path, None, maximum, &mut root_accounting(io)?)
                    .await
                    .map(|(chunk, _)| chunk.bytes)
                    .map_err(Error::from)
            })
            .map(crate::store::history::HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            let mut accounting = root_accounting(io)?;
            let result = self
                .lazy
                .read_at_snapshot(snapshot, path, None, maximum, &mut accounting)
                .await
                .map(|(chunk, _)| chunk.bytes);
            receive_root(result, accounting, io)
        })
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
            self.lazy
                .list_directory_at_snapshot(
                    snapshot,
                    path,
                    cursor,
                    maximum,
                    budget,
                    cancellation,
                    None,
                )
                .await
                .map_err(|failure| failure.map_error(root_source_error))
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
        crate::store::history::HistoryContentFuture<
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
                Err(crate::filesystem::fork_preparer_io::failure(
                    Some(io),
                    crate::wire::ErrorCode::Conflict,
                    "Root upload publication was rejected",
                ))
            }
        };
        if let Some(original) = io.original_native_work() {
            return crate::NativeFutureBundle::new(original, || async move {
                let lease = Arc::clone(lease);
                let mut before_mutation = move |work: &acyclic_fs::NativeMaterializationWork| {
                    let original = acyclic_fs::NativeMaterializationWork::clone(work);
                    let lease = Arc::clone(&lease);
                    async move { lease.verify_with_native_work(&original)?.await }
                };
                let (commit, snapshot) = (async {
                    let cancellation = io.cancellation();
                    let accounting = acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(
                        original,
                        cancellation,
                    )?;
                    let mut guard = self
                        .lazy
                        .native_write_guard(&mut before_mutation, &accounting)?;
                    self.lazy
                        .write_at_snapshot(
                            snapshot,
                            root_lazy_mutations(mutations),
                            key,
                            permit,
                            &accounting,
                            &mut guard,
                        )
                        .await
                })
                .await?;
                Ok((generation(commit)?, snapshot))
            })
            .map(crate::store::history::HistoryContentFuture::Native);
        }
        io.history_future(|| async move {
            let allowance = io.remaining()?;
            let mut before_mutation = |budget| async move {
                let callback =
                    crate::filesystem::PreparationIo::from_budget(budget, io.cancellation())
                        .map_err(acyclic_fs::OperationFailure::before_work)?;
                let verified = callback.verify_publication_lease(lease.as_ref()).await;
                match verified {
                    Ok(()) => Ok(acyclic_fs::OperationReceipt {
                        value: (),
                        work: callback.receipt(),
                    }),
                    Err(error) => Err(acyclic_fs::OperationFailure::new(error, callback.receipt())),
                }
            };
            let accounting =
                acyclic_fs::lazy_workspace::LazyTreapWork::new(allowance, io.cancellation());
            let result = self
                .lazy
                .write_at_snapshot(
                    snapshot,
                    root_lazy_mutations(mutations),
                    key,
                    permit,
                    &accounting,
                    &mut before_mutation,
                )
                .await;
            let (commit, snapshot) =
                io.receive_content(accounting.measured(result, |error, spent| {
                    acyclic_fs::OperationFailure::new(error, spent)
                }))?;
            Ok((generation(commit)?, snapshot))
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

fn root_accounting(
    io: &crate::filesystem::PreparationIo,
) -> Result<acyclic_fs::kernel::KernelAccounting<'_>> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.original_native_work() {
        return Ok(acyclic_fs::kernel::KernelAccounting::NativeOriginal {
            work,
            cancellation: io.cancellation(),
        });
    }
    Ok(acyclic_fs::kernel::KernelAccounting::Standalone {
        allocations: Default::default(),
        work: WorkCounters::UNCHARGED,
        budget: io.remaining()?,
        cancellation: io.cancellation(),
    })
}
fn receive_root<T>(
    result: std::result::Result<T, acyclic_fs::LazyWorkspaceError>,
    accounting: acyclic_fs::kernel::KernelAccounting<'_>,
    io: &crate::filesystem::PreparationIo,
) -> Result<T> {
    if let acyclic_fs::kernel::KernelAccounting::Standalone { work, .. } = accounting {
        return io.receive_content(
            result
                .map(|value| acyclic_fs::OperationReceipt { value, work })
                .map_err(|error| acyclic_fs::OperationFailure::new(root_source_error(error), work)),
        );
    }
    result.map_err(root_source_error)
}
