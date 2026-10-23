use super::*;

#[derive(Clone, Copy)]
enum FilePublication<'a> {
    Replace(Option<&'a GenerationRef>),
    Authorized(acyclic_fs::access::PolicyFence),
    CreateOnly,
}

impl FilePublication<'_> {
    fn fence(self) -> Option<acyclic_fs::access::PolicyFence> {
        match self {
            Self::Authorized(fence) => Some(fence),
            Self::Replace(_) | Self::CreateOnly => None,
        }
    }
}

fn require_publication_time(fence: Option<acyclic_fs::access::PolicyFence>) -> Result<()> {
    if fence.is_some_and(|fence| !fence.valid_at(access::access_now())) {
        return Err(Error::Unauthorized(
            "filesystem write authority expired".into(),
        ));
    }
    Ok(())
}

fn creation_error(error: WorkspaceError, io: Option<&PreparationIo>) -> Error {
    match error {
        WorkspaceError::Filesystem(acyclic_fs::FsError::Mutation(
            acyclic_fs::kernel::GenerationMutationError::AlreadyExists,
        )) => stock_boundary(
            io,
            crate::wire::ErrorCode::Conflict,
            "immutable workspace record already exists",
        ),
        error => error.into(),
    }
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

    pub(in crate::filesystem) async fn retain_generation(
        &self,
        workspace: &WorkspaceRef,
        reference: &GenerationRef,
    ) -> Result<()> {
        #[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
        acyclic_differential::record(("retain", 0u64));
        let workspace = self.open(workspace, None).await?;
        let generation = self.generation(&workspace, reference, None).await?;
        let identity = format!(
            "harness-generation-{}",
            hex::encode(reference.as_resource().key())
        );
        generation
            .retain_if_unpublished(
                identity,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?;
        Ok(())
    }

    pub(in crate::filesystem) fn generation_ref_id(
        &self,
        id: GenerationId,
        io: Option<&PreparationIo>,
    ) -> Result<GenerationRef> {
        fork_preparer_io::generation_ref(&self.provider, id, io)
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
        let generation = crate::conversation::BoundedContentFuture::new(None, || {
            self.apply_generation_unmeasured(
                workspace,
                expected_generation,
                mutations,
                idempotency_key,
                true,
                FilePublication::Replace(None),
            )
        })?
        .await?;
        self.generation_ref_id(generation.id(), None)
    }

    async fn apply_generation_unmeasured(
        &self,
        workspace: &WorkspaceRef,
        expected_generation: Option<&GenerationRef>,
        mutations: &[WorkspaceMutation],
        idempotency_key: &IdempotencyKey,
        invalidate_metadata: bool,
        publication: FilePublication<'_>,
    ) -> Result<Generation<A, O>> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let budget = acyclic_fs::WorkBudget::UNBOUNDED;
        let workspace = self.open(workspace, None).await?;
        let key = filesystem_key(idempotency_key);
        let mut transaction = match expected_generation {
            Some(reference) => {
                let generation = self.generation(&workspace, reference, None).await?;
                workspace
                    .begin_transaction_at(&generation, key, budget, &cancellation)
                    .await
                    .map(|receipt| receipt.value)
                    .map_err(|failure| failure.error)
                    .map_err(map_error)?
            }
            None => workspace
                .begin_transaction(key, budget, &cancellation)
                .await
                .map(|receipt| receipt.value)
                .map_err(|failure| failure.error)
                .map_err(map_error)?,
        };
        if let Some(fence) = publication.fence() {
            transaction
                .observe_policy_fence(fence, budget, &cancellation)
                .await
                .map_err(|failure| map_error(failure.error))?;
        }
        Self::stage_unmeasured(
            &mut transaction,
            mutations,
            invalidate_metadata,
            publication,
            budget,
            &cancellation,
        )
        .await?;
        loop {
            let committed = transaction
                .commit(
                    None,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    budget,
                    &cancellation,
                    |_| async {
                        require_publication_time(publication.fence())
                            .map_err(acyclic_fs::OperationFailure::before_work)?;
                        Ok::<_, acyclic_fs::OperationFailure<Error>>(acyclic_fs::OperationReceipt {
                            value: (),
                            work: acyclic_fs::WorkCounters::default(),
                        })
                    },
                )
                .await
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
                    match transaction
                        .rebase(1, budget, &cancellation)
                        .await
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
    }

    async fn stage_unmeasured(
        transaction: &mut acyclic_fs::Transaction<A, O>,
        mutations: &[WorkspaceMutation],
        invalidate_metadata: bool,
        publication: FilePublication<'_>,
        budget: acyclic_fs::WorkBudget,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        let batched = if !invalidate_metadata
            && !matches!(publication, FilePublication::CreateOnly)
            && mutations
                .iter()
                .all(|mutation| !matches!(mutation, WorkspaceMutation::Remove { .. }))
        {
            let directories = mutations
                .iter()
                .filter_map(|mutation| match mutation {
                    WorkspaceMutation::CreateDirectory { path } => Some(path.as_str()),
                    _ => None,
                })
                .collect::<Vec<_>>();
            let files = mutations
                .iter()
                .filter_map(|mutation| match mutation {
                    WorkspaceMutation::PutFile { path, bytes } => {
                        Some((path.as_str(), bytes.clone()))
                    }
                    _ => None,
                })
                .collect::<Vec<_>>();
            transaction
                .write_batch(&directories, &files, budget, cancellation)
                .await
                .map_err(|failure| map_error(failure.error))?;
            true
        } else {
            false
        };
        #[cfg(target_arch = "wasm32")]
        let batched = false;
        if !batched {
            for mutation in mutations {
                match mutation {
                    WorkspaceMutation::CreateDirectory { path } => {
                        transaction
                            .create_dir_all(path, budget, cancellation)
                            .await
                            .map(|receipt| receipt.value)
                            .map_err(|failure| failure.error)
                            .map_err(map_error)?;
                    }
                    WorkspaceMutation::PutFile { path, bytes } => {
                        match publication {
                            FilePublication::Replace(_) | FilePublication::Authorized(_) => {
                                transaction
                                    .write(path, bytes.clone(), budget, cancellation)
                                    .await
                                    .map_err(|failure| map_error(failure.error))?
                            }
                            FilePublication::CreateOnly => transaction
                                .create_file(
                                    path,
                                    bytes.clone(),
                                    acyclic_fs::kernel::FileMetadata::default(),
                                    budget,
                                    cancellation,
                                )
                                .await
                                .map_err(|failure| creation_error(failure.error, None))?,
                        };
                    }
                    WorkspaceMutation::Remove { path } => {
                        transaction
                            .remove(path, budget, cancellation)
                            .await
                            .map(|receipt| receipt.value)
                            .map_err(|failure| failure.error)
                            .map_err(map_error)?;
                    }
                }
                // Raw mutations do not carry a descriptor or write publication.
                // Invalidate the old index atomically, including same-byte writes.
                // The content writer supplies its own metadata in this transaction.
                if invalidate_metadata
                    && let WorkspaceMutation::PutFile { path, .. }
                    | WorkspaceMutation::Remove { path } = mutation
                {
                    let metadata =
                        content_metadata_path_io(path.trim_start_matches('/'), true, None)?;
                    match transaction.remove(&metadata, budget, cancellation).await {
                        Ok(_) => {}
                        Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {}
                        Err(failure) => return Err(map_error(failure.error)),
                    }
                }
            }
        }
        Ok(())
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

    pub(in crate::filesystem) fn apply_generation<'a>(
        &'a self,
        reference: &'a WorkspaceRef,
        expected: Option<&'a GenerationRef>,
        mutations: &'a [WorkspaceMutation],
        key: IdempotencyKey,
        io: Option<&'a PreparationIo>,
        preparation: Option<&'a Arc<crate::resource_preparation::TaskResourcePreparation>>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>> {
        self.publish_generation(
            reference,
            mutations,
            key,
            io,
            preparation,
            FilePublication::Replace(expected),
        )
    }

    pub(in crate::filesystem) fn apply_authorized_generation<'a>(
        &'a self,
        reference: &'a WorkspaceRef,
        fence: acyclic_fs::access::PolicyFence,
        mutations: &'a [WorkspaceMutation],
        key: IdempotencyKey,
        io: Option<&'a PreparationIo>,
        preparation: Option<&'a Arc<crate::resource_preparation::TaskResourcePreparation>>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>> {
        self.publish_generation(
            reference,
            mutations,
            key,
            io,
            preparation,
            FilePublication::Authorized(fence),
        )
    }

    pub(in crate::filesystem) fn create_records<'a>(
        &'a self,
        reference: &'a WorkspaceRef,
        mutations: &'a [WorkspaceMutation],
        key: IdempotencyKey,
        io: Option<&'a PreparationIo>,
        preparation: Option<&'a Arc<crate::resource_preparation::TaskResourcePreparation>>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>> {
        self.publish_generation(
            reference,
            mutations,
            key,
            io,
            preparation,
            FilePublication::CreateOnly,
        )
    }

    fn publish_generation<'a>(
        &'a self,
        reference: &'a WorkspaceRef,
        mutations: &'a [WorkspaceMutation],
        key: IdempotencyKey,
        io: Option<&'a PreparationIo>,
        preparation: Option<&'a Arc<crate::resource_preparation::TaskResourcePreparation>>,
        publication: FilePublication<'a>,
    ) -> Result<crate::conversation::BoundedContentFuture<'a, Generation<A, O>>> {
        let expected = match publication {
            FilePublication::Replace(expected) => expected,
            FilePublication::CreateOnly | FilePublication::Authorized(_) => None,
        };
        let Some(io) = io else {
            return crate::conversation::BoundedContentFuture::new(None, || async move {
                self.apply_generation_unmeasured(
                    reference,
                    expected,
                    mutations,
                    &key,
                    false,
                    publication,
                )
                .await
            });
        };
        crate::conversation::BoundedContentFuture::new(Some(io), || async move {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.original_native_work() {
                verify_native_publication(preparation, work).await?;
            }
            let workspace = self.open(reference, Some(io)).await?;
            let operation = fork_preparer_io::operation_key(&key, Some(io))?;
            let mut transaction = if !matches!(publication, FilePublication::Replace(_)) {
                io.sdk(
                    workspace
                        .begin_transaction(operation, io, io.cancellation())
                        .await,
                )?
            } else {
                let pinned = match expected {
                    Some(reference) => self.generation(&workspace, reference, Some(io)).await?,
                    None => io.sdk(workspace.head(io, io.cancellation()).await)?,
                };
                io.sdk(
                    workspace
                        .begin_transaction_at(&pinned, operation, io, io.cancellation())
                        .await,
                )?
            };
            if let Some(fence) = publication.fence() {
                io.sdk(
                    transaction
                        .observe_policy_fence(fence, io, io.cancellation())
                        .await,
                )?;
            }
            #[cfg(test)]
            if let Some(pause) = &self.publication_pause {
                pause().await;
            }
            Self::publish_mutations(&mut transaction, mutations, publication, io, preparation)
                .await?;
            Self::commit_publication(transaction, io, preparation, publication.fence()).await
        })
    }
    async fn publish_mutations(
        transaction: &mut acyclic_fs::Transaction<A, O>,
        mutations: &[WorkspaceMutation],
        publication: FilePublication<'_>,
        io: &PreparationIo,
        preparation: Option<&Arc<crate::resource_preparation::TaskResourcePreparation>>,
    ) -> Result<()> {
        crate::conversation::BoundedContentFuture::new(Some(io), || async {
            for mutation in mutations {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.original_native_work() {
                    verify_native_publication(preparation, work).await?;
                }
                match mutation {
                    WorkspaceMutation::CreateDirectory { path } => io.sdk(
                        transaction
                            .create_dir_all(path, io, io.cancellation())
                            .await,
                    )?,
                    WorkspaceMutation::PutFile { path, bytes } => match publication {
                        FilePublication::Replace(_) | FilePublication::Authorized(_) => io.sdk(
                            transaction
                                .write(path, bytes.clone(), io, io.cancellation())
                                .await,
                        )?,
                        FilePublication::CreateOnly => {
                            let (created, recorded) = transaction
                                .create_file(
                                    path,
                                    bytes.clone(),
                                    acyclic_fs::kernel::FileMetadata::default(),
                                    io,
                                    io.cancellation(),
                                )
                                .await;
                            recorded?;
                            io.sdk((
                                created.map_err(|error| creation_error(error, Some(io))),
                                Ok(()),
                            ))?;
                        }
                    },
                    WorkspaceMutation::Remove { .. } => {
                        return Err(stock_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Unsupported,
                            "bounded publication does not remove content",
                        ));
                    }
                }
            }
            Ok(())
        })?
        .await
    }

    async fn commit_publication(
        mut transaction: acyclic_fs::Transaction<A, O>,
        io: &PreparationIo,
        preparation: Option<&Arc<crate::resource_preparation::TaskResourcePreparation>>,
        fence: Option<acyclic_fs::access::PolicyFence>,
    ) -> Result<Generation<A, O>> {
        let commit = crate::conversation::BoundedContentFuture::new(Some(io), || async {
            let commit = 'commit: {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.original_native_work() {
                    let preparation = preparation.cloned();
                    let mut native_before = move |work: &acyclic_fs::NativeMaterializationWork| {
                        let preparation = preparation.clone();
                        let work = work.clone();
                        async move {
                            require_publication_time(fence)?;
                            verify_native_publication(preparation.as_ref(), &work).await?;
                            require_publication_time(fence)
                        }
                    };
                    break 'commit transaction
                        .commit_with_native_work(
                            None,
                            acyclic_fs::PublicationPermit::Unrestricted,
                            work,
                            io.cancellation(),
                            &mut native_before,
                        )
                        .await?;
                }
                break 'commit io.receive(
                    transaction
                        .commit(
                            None,
                            acyclic_fs::PublicationPermit::Unrestricted,
                            io.remaining()?,
                            io.cancellation(),
                            &mut |_| async move {
                                require_publication_time(fence)
                                    .map_err(acyclic_fs::OperationFailure::before_work)?;
                                if let Some(task) = preparation {
                                    task.verify(false, Some(io))
                                        .map_err(acyclic_fs::OperationFailure::before_work)?
                                        .await
                                        .map_err(acyclic_fs::OperationFailure::before_work)?;
                                }
                                require_publication_time(fence)
                                    .map_err(acyclic_fs::OperationFailure::before_work)?;
                                Ok(acyclic_fs::OperationReceipt {
                                    value: (),
                                    work: acyclic_fs::WorkCounters::default(),
                                })
                            },
                        )
                        .await,
                )?;
            };
            Ok(commit)
        })?
        .await?;
        match commit {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
            TransactionCommit::Conflict { .. }
            | TransactionCommit::Fenced
            | TransactionCommit::IdempotencyConflict => Err(stock_boundary(
                Some(io),
                crate::wire::ErrorCode::Conflict,
                "workspace publication conflicted or was fenced",
            )),
        }
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
        let key = fork_preparer_io::operation_key(key, Some(io))?;
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

#[cfg(not(target_arch = "wasm32"))]
async fn verify_native_publication(
    preparation: Option<&Arc<crate::resource_preparation::TaskResourcePreparation>>,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    if let Some(task) = preparation {
        let io = PreparationIo::from_native_work(work)?;
        task.verify(false, Some(&io))?.await?;
    }
    Ok(())
}
