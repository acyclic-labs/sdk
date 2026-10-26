use super::*;

#[cfg(not(target_arch = "wasm32"))]
#[path = "publication_batch.rs"]
mod batch;
mod commit;

type WorkspaceOperation<'a, T, E = WorkspaceError> = crate::conversation::BoundedContentFuture<
    'a,
    acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, E>,
>;

// Keep SDK result/receipt mapping at each caller, while constructing its
// concrete operation before the caller's poll frame is entered again.
#[inline(never)]
fn unmeasured_workspace_operation<'a, T: 'a, E: 'a, F>(
    build: impl FnOnce() -> F,
) -> Result<WorkspaceOperation<'a, T, E>>
where
    F: Future<Output = acyclic_fs::MeasuredResult<acyclic_fs::OperationReceipt<T>, E>>
        + acyclic_stream::ProviderTask
        + 'a,
{
    crate::conversation::BoundedContentFuture::new(None, || futures::FutureExt::map(build(), Ok))
}

#[derive(Clone, Copy)]
enum FilePublication<'a> {
    Replace(Option<&'a GenerationRef>),
    Authorized(acyclic_fs::access::PolicyFence, &'a ContentGrant),
}

impl FilePublication<'_> {
    fn fence(self) -> Option<acyclic_fs::access::PolicyFence> {
        match self {
            Self::Authorized(fence, _) => Some(fence),
            Self::Replace(_) => None,
        }
    }
}

fn require_publication_time(
    publication: FilePublication<'_>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    if let FilePublication::Authorized(_, grant) = publication {
        grant.recheck(io)?;
    }
    require_publication_fence(publication.fence())
}

fn require_publication_fence(fence: Option<acyclic_fs::access::PolicyFence>) -> Result<()> {
    let now = access::access_now()?;
    if fence.is_some_and(|fence| !fence.valid_at(now)) {
        return Err(Error::Unauthorized(
            "filesystem write authority expired".into(),
        ));
    }
    Ok(())
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Reads one bounded file from the current head or an exact generation.
    pub async fn read(
        &self,
        reference: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("read", maximum_bytes));
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        let workspace = self.open(reference, Some(io)).await?;
        let bytes = match generation {
            Some(reference) => io.sdk(
                self.generation(&workspace, reference, Some(io))
                    .await?
                    .read(path, maximum_bytes, io, io.cancellation())
                    .await,
            )?,
            None => io.sdk(
                workspace
                    .read(path, maximum_bytes, io, io.cancellation())
                    .await,
            )?,
        };
        if io.original_native_work().is_none() {
            io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?;
        }
        Ok(bytes)
    }

    /// Stats one path at the current head or an exact generation.
    pub async fn stat(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
    ) -> Result<WorkspaceStat> {
        let workspace = self.open(workspace, None).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference, None)
                .await?
                .stat(
                    path,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| map_error(failure.error)),
            None => workspace
                .stat(
                    path,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| map_error(failure.error)),
        }
    }

    /// Lists one bounded directory page at the current head or an exact generation.
    pub async fn list(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        maximum_entries: u32,
    ) -> Result<DirectoryPage> {
        self.list_after(workspace, generation, path, None, maximum_entries)
            .await
    }

    /// Lists one bounded page after an exact child name cursor.
    pub async fn list_after(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        after: Option<&LogicalName>,
        maximum_entries: u32,
    ) -> Result<DirectoryPage> {
        let workspace = self.open(workspace, None).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference, None)
                .await?
                .list_directory(
                    path,
                    after,
                    maximum_entries,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value.0)
                .map_err(|failure| map_error(failure.error)),
            None => workspace
                .list_directory(
                    path,
                    after,
                    maximum_entries,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map(|receipt| receipt.value.0)
                .map_err(|failure| map_error(failure.error)),
        }
    }

    pub(in crate::filesystem) fn generation_ref_id(
        &self,
        id: GenerationId,
        io: Option<&PreparationIo>,
    ) -> Result<GenerationRef> {
        bounded_io::generation_ref(&self.provider, id, io)
    }

    pub(in crate::filesystem) fn validate_provider(&self, provider: &ProviderRef) -> Result<()> {
        provider.validate(&mut None)?;
        if provider != &self.provider {
            return Err(Error::Invalid(
                "resource belongs to another Filesystem provider".into(),
            ));
        }
        Ok(())
    }

    /// Publishes only to the authorized destination using a stable retry key.
    pub async fn apply(
        &self,
        workspace: &WorkspaceRef,
        expected_generation: Option<&GenerationRef>,
        mutations: &[WorkspaceMutation],
        idempotency_key: &IdempotencyKey,
    ) -> Result<GenerationRef> {
        let generation = self
            .apply_generation_unmeasured(
                workspace,
                expected_generation,
                mutations,
                idempotency_key,
                true,
                FilePublication::Replace(None),
            )?
            .await?;
        self.generation_ref_id(generation.id(), None)
    }

    fn apply_generation_unmeasured<'a>(
        &'a self,
        workspace: &'a WorkspaceRef,
        expected_generation: Option<&'a GenerationRef>,
        mutations: &'a [WorkspaceMutation],
        idempotency_key: &'a IdempotencyKey,
        invalidate_metadata: bool,
        publication: FilePublication<'a>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>> {
        // Construction returns before polling this concrete publication graph.
        crate::conversation::BoundedContentFuture::new(None, || async move {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::WorkBudget::UNBOUNDED;
            let workspace = self.open(workspace, None).await?;
            let key = filesystem_key(idempotency_key);
            let mut transaction = match expected_generation {
                Some(reference) => {
                    let generation = self.generation(&workspace, reference, None).await?;
                    unmeasured_workspace_operation(|| {
                        workspace.begin_transaction_at(&generation, key, budget, &cancellation)
                    })?
                    .await?
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                    .map_err(map_error)?
                }
                None => unmeasured_workspace_operation(|| {
                    workspace.begin_transaction(key, budget, &cancellation)
                })?
                .await?
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .map_err(map_error)?,
            };
            if let Some(fence) = publication.fence() {
                unmeasured_workspace_operation(|| {
                    transaction.observe_policy_fence(fence, budget, &cancellation)
                })?
                .await?
                .map_err(|failure| map_error(failure.error))?;
            }
            Self::stage_unmeasured(
                &mut transaction,
                mutations,
                invalidate_metadata,
                budget,
                &cancellation,
            )?
            .await?;
            loop {
                let committed = unmeasured_workspace_operation(|| {
                    transaction.commit(
                        None,
                        acyclic_fs::PublicationPermit::Unrestricted,
                        budget,
                        &cancellation,
                        |_| async {
                            require_publication_time(publication, None)
                                .map_err(acyclic_fs::OperationFailure::before_work)?;
                            Ok::<_, acyclic_fs::OperationFailure<Error>>(
                                acyclic_fs::OperationReceipt {
                                    value: (),
                                    work: acyclic_fs::WorkCounters::default(),
                                },
                            )
                        },
                    )
                })?
                .await?
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)?;
                return match committed {
                    TransactionCommit::Committed(generation)
                    | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
                    TransactionCommit::Conflict { .. }
                        if expected_generation.is_none()
                            && publication
                                .fence()
                                .is_none_or(|fence| !fence.generation_sensitive()) =>
                    {
                        // Reuse the original candidate only when the SDK proves its
                        // exact observations and mutation dependencies still hold.
                        match unmeasured_workspace_operation(|| {
                            transaction.rebase(1, budget, &cancellation)
                        })?
                        .await?
                        .map(|receipt| receipt.value)
                        .map_err(|failure| map_error(failure.error))?
                        {
                            acyclic_fs::TransactionRebase::Rebased(_) => continue,
                            acyclic_fs::TransactionRebase::Conflicted { .. } => {
                                Err(Error::Conflict("workspace generation changed".into()))
                            }
                        }
                    }
                    TransactionCommit::Conflict { .. } => {
                        Err(Error::Conflict("workspace generation changed".into()))
                    }
                    TransactionCommit::Fenced => {
                        Err(Error::Conflict("workspace writer was fenced".into()))
                    }
                    TransactionCommit::IdempotencyConflict => Err(Error::Conflict(
                        "filesystem idempotency key belongs to another mutation".into(),
                    )),
                };
            }
        })
    }

    async fn stage_mutation_unmeasured(
        transaction: &mut acyclic_fs::Transaction<A, O>,
        mutation: &WorkspaceMutation,
        invalidate_metadata: bool,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<()> {
        match mutation {
            WorkspaceMutation::CreateDirectory { path } => {
                unmeasured_workspace_operation(|| {
                    transaction.create_dir_all(path, budget, cancellation)
                })?
                .await?
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .map_err(map_error)?;
            }
            WorkspaceMutation::PutFile { path, bytes } => {
                unmeasured_workspace_operation(|| {
                    transaction.write(path, bytes.clone(), budget, cancellation)
                })?
                .await?
                .map_err(|failure| map_error(failure.error))?;
            }
            WorkspaceMutation::Remove { path } => {
                unmeasured_workspace_operation(|| transaction.remove(path, budget, cancellation))?
                    .await?
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                    .map_err(map_error)?;
            }
        }
        // Raw mutations do not carry a descriptor or write publication.
        // Invalidate the old index atomically, including same-byte writes.
        // The content writer supplies its own metadata in this transaction.
        if invalidate_metadata
            && let WorkspaceMutation::PutFile { path, .. } | WorkspaceMutation::Remove { path } =
                mutation
        {
            let metadata = content_metadata_path_io(path.trim_start_matches('/'), true, None)?;
            match unmeasured_workspace_operation(|| {
                transaction.remove(&metadata, budget, cancellation)
            })?
            .await?
            {
                Ok(_) => {}
                Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {}
                Err(failure) => return Err(map_error(failure.error)),
            }
        }
        Ok(())
    }

    fn stage_unmeasured<'a>(
        transaction: &'a mut acyclic_fs::Transaction<A, O>,
        mutations: &'a [WorkspaceMutation],
        invalidate_metadata: bool,
        budget: acyclic_fs::WorkBudget,
        cancellation: &'a acyclic_fs::CancellationToken,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, ()>> {
        // Construction returns before polling this concrete publication graph.
        crate::conversation::BoundedContentFuture::new(None, || async move {
            #[cfg(not(target_arch = "wasm32"))]
            let batched = !invalidate_metadata
                && crate::conversation::BoundedContentFuture::new(None, || {
                    Self::stage_publication_batch(transaction, mutations, None, cancellation)
                })?
                .await?;
            #[cfg(target_arch = "wasm32")]
            let batched = false;
            if !batched {
                for mutation in mutations {
                    Self::stage_mutation_unmeasured(
                        transaction,
                        mutation,
                        invalidate_metadata,
                        budget,
                        cancellation,
                    )
                    .await?;
                }
            }
            Ok(())
        })
    }

    /// Resolves the current immutable head under the caller's work context.
    pub async fn resolve(
        &self,
        reference: &WorkspaceRef,
        io: Option<&PreparationIo>,
    ) -> Result<WorkspaceObservation> {
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        let workspace = self.open(reference, Some(io)).await?;
        let head = io.sdk(workspace.head(io, io.cancellation()).await)?;
        let reference = crate::executor::with_history_io(Some(io), |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let resource = crate::core::copy_resource(reference.as_resource(), &mut quota)?;
            quota.transfer();
            drop(quota);
            WorkspaceRef::from_resource(resource, work)
        })?;
        Ok(WorkspaceObservation {
            workspace: reference,
            generation: self.generation_ref_id(head.id(), Some(io))?,
        })
    }

    pub(in crate::filesystem) async fn open<'a>(
        &'a self,
        reference: &'a WorkspaceRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<Workspace<A, O>> {
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        reference.validate(&mut Some(&mut io.history_io()))?;
        if reference.as_resource().provider() != &self.provider {
            return Err(io.invalid("resource belongs to another Filesystem provider"));
        }
        if let Some((bound, workspace)) = &self.workspace_binding
            && bound == reference
        {
            return Ok(workspace.clone());
        }
        let name = std::str::from_utf8(reference.as_resource().key())
            .map_err(|_| io.invalid("workspace reference key must be UTF-8"))?;
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("open workspace", 1u64));
        io.sdk(
            self.filesystem
                .open_workspace(name, io, io.cancellation())
                .await,
        )
    }

    pub(in crate::filesystem) async fn generation<'a>(
        &'a self,
        workspace: &'a Workspace<A, O>,
        reference: &'a GenerationRef,
        io: Option<&'a PreparationIo>,
    ) -> Result<Generation<A, O>> {
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        reference.validate(&mut Some(&mut io.history_io()))?;
        if reference.as_resource().provider() != &self.provider {
            return Err(io.invalid("resource belongs to another Filesystem provider"));
        }
        let key = reference
            .as_resource()
            .key()
            .try_into()
            .map_err(|_| io.invalid("generation reference key must be 32 bytes"))?;
        let generation = GenerationId::new(Digest::from_bytes(key));
        self.select_generation(workspace, generation, io).await
    }

    pub(in crate::filesystem) async fn select_generation(
        &self,
        workspace: &Workspace<A, O>,
        generation: GenerationId,
        io: &PreparationIo,
    ) -> Result<Generation<A, O>> {
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        if let Some((root, store)) = &self.project_family
            && workspace.id() == *root
        {
            return crate::conversation::BoundedContentFuture::new(Some(io), || {
                self.project_generation(workspace, generation, *root, store, io)
            })?
            .await;
        }

        io.sdk(
            workspace
                .generation(generation, io, io.cancellation())
                .await,
        )
    }

    pub(in crate::filesystem) async fn operation_generation(
        &self,
        reference: &WorkspaceRef,
        key: &IdempotencyKey,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Generation<A, O>>> {
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        let workspace = self.open(reference, Some(io)).await?;
        let key = bounded_io::operation_key(key, Some(io))?;
        io.sdk(
            workspace
                .operation_generation(key, io, io.cancellation())
                .await,
        )
    }
}

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
mod bytes_tests;

#[cfg(test)]
mod workload_tests;
