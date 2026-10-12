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
            use acyclic_fs::facade::FsWork;
            let mut accounting = budget.accounting(cancellation);
            let result = async {
                self.lazy.validate_snapshot(snapshot, &mut accounting)?;
                let state = acyclic_fs::LazyWorkspaceState::new(
                    snapshot.workspace_id, snapshot.source, snapshot.overlay, snapshot.shadows,
                );
                let mut checkout = self.lazy.checkout(
                    acyclic_fs::model::GenerationSelector::Exact(snapshot.authored_generation),
                    &mut accounting,
                ).await?;
                let result = self.lazy.list_directory_at(
                    &state, &mut checkout.value, path, cursor, maximum, None, &mut accounting,
                ).await;
                checkout.retire(&mut accounting)?;
                result.map(|(page, _, allocation)| { drop(allocation); page })
            }.await;
            budget.finish(result, &accounting)
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
        let publish = || async move {
            use acyclic_fs::facade::FsWork;
            use crate::filesystem::WorkspaceMutation;
            lease.verify(Some(io))?.await?;
            let mut accounting = (&io).accounting(io.cancellation());
            let result = self.lazy.validate_snapshot(snapshot, &mut accounting);
            let (result, recorded) = (&io).finish(result, &accounting);
            recorded?;
            result.map_err(root_source_error)?;
            let workspace = self.lazy.workspace();
            let pinned = io.sdk(workspace.generation(snapshot.authored_generation, io, io.cancellation()).await)?;
            let mut transaction = io.sdk(workspace.begin_pinned_transaction(&pinned, key, io, io.cancellation()).await)?;
            for mutation in mutations {
                lease.verify(Some(io))?.await?;
                let mut identity = None;
                if let WorkspaceMutation::PutFile { path, .. } = mutation {
                    if let Some((parent, _)) = path.rsplit_once('/') {
                        let parent = if parent.is_empty() { "/" } else { parent };
                        io.sdk(transaction.create_dir_all(parent, io, io.cancellation()).await)?;
                    }
                    let mut accounting = (&io).accounting(io.cancellation());
                    let result = async {
                        let (lookup, _, allocation) = self.lazy.lookup_at_snapshot(snapshot, path, &mut accounting).await?;
                        if let Some(lookup) = lookup {
                            let result = async {
                                match &lookup {
                                    acyclic_fs::LazyLookup::Source { node, .. } => {
                                        identity = Some(self.lazy.source_file_id(&node.file_identity, &mut accounting)?);
                                    }
                                    _ => { self.lazy.apply_resolved_promotion(&mut transaction, path, &lookup, u64::MAX, &mut accounting).await?; }
                                }
                                Ok::<_, acyclic_fs::LazyWorkspaceError>(())
                            }.await;
                            self.lazy.retire_lookup((lookup, allocation), &mut accounting)?;
                            result?;
                        }
                        Ok::<_, acyclic_fs::LazyWorkspaceError>(())
                    }.await;
                    let (result, recorded) = (&io).finish(result, &accounting);
                    recorded?;
                    result.map_err(root_source_error)?;
                }
                crate::filesystem::fork_preparer_io::mutation(&mut transaction, mutation, io).await?;
                if let (WorkspaceMutation::PutFile { path, .. }, Some(identity)) = (mutation, identity) {
                    io.sdk(transaction.preserve_file_identity(path, identity, io, io.cancellation()).await)?;
                }
            }
            let commit = if let Some(original) = io.original_native_work() {
                let lease = Arc::clone(lease);
                let mut before = move |work: &acyclic_fs::NativeMaterializationWork| {
                    let original = work.clone();
                    let lease = Arc::clone(&lease);
                    async move {
                        let io = crate::filesystem::PreparationIo::from_native_work(&original)?;
                        lease.verify(Some(&io))?.await
                    }
                };
                transaction.commit_with_native_work(None, permit, original, io.cancellation(), &mut before).await?
            } else {
                let before = |budget| async move {
                    let callback = crate::filesystem::PreparationIo::from_budget(budget, io.cancellation())
                        .map_err(acyclic_fs::OperationFailure::before_work)?;
                    let result = match lease.verify(Some(&callback)) {
                        Ok(future) => future.await,
                        Err(error) => Err(error),
                    };
                    result.map(|value| acyclic_fs::OperationReceipt { value, work: callback.receipt() })
                        .map_err(|error| acyclic_fs::OperationFailure::new(error, callback.receipt()))
                };
                io.receive_content(transaction.commit(None, permit, io.remaining()?, io.cancellation(), before).await)?
            };
            let generation = generation(commit)?;
            let updated = io.charge(WorkCounters {
                bytes_hashed: acyclic_fs::lazy_workspace::LAZY_SNAPSHOT_DOMAIN.len() as u64 + 136,
                ..WorkCounters::default()
            }).map(|()| LazySnapshotRef::new(snapshot.workspace_id, generation.id(), snapshot.source, snapshot.overlay, snapshot.shadows));
            Ok((generation, updated.map_err(Into::into)))
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
