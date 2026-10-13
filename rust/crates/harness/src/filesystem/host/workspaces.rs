use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Resolves the current immutable head of a named workspace.


    /// Reads one bounded file from the current head or an exact generation.
    pub async fn read(&self, reference: &WorkspaceRef, generation: Option<&GenerationRef>, path: &str, maximum_bytes: u64, io: Option<&PreparationIo>) -> Result<acyclic_stream::OwnedBytes> {
        let owned = if io.is_none() { Some(PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?) } else { None };
        let io = io.or(owned.as_ref()).expect("work context");
        let workspace = self.open(reference, Some(io)).await?;
        let bytes = match generation {
            Some(reference) => io.sdk(self.generation(&workspace, reference, Some(io)).await?.read(path, maximum_bytes, io, io.cancellation()).await)?,
            None => io.sdk(workspace.read(path, maximum_bytes, io, io.cancellation()).await)?,
        };
        if io.original_native_work().is_none() { io.retain_owned(bytes.retained_capacity().unwrap_or(0) as u64)?; }
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
                .stat(path, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                .await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error)),
            None => workspace.stat(path, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error)),
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
                .list_directory(path, after, maximum_entries, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                .await.map(|receipt| receipt.value.0).map_err(|failure| map_error(failure.error)),
            None => workspace
                .list_directory(path, after, maximum_entries, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                .await.map(|receipt| receipt.value.0).map_err(|failure| map_error(failure.error)),
        }
    }





    pub(in crate::filesystem) async fn retain_generation(
        &self,
        workspace: &WorkspaceRef,
        reference: &GenerationRef,
    ) -> Result<()> {
        let workspace = self.open(workspace, None).await?;
        let generation = self.generation(&workspace, reference, None).await?;
        let identity = format!(
            "harness-generation-{}",
            hex::encode(reference.as_resource().key())
        );
        generation.pin(identity, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?;
        Ok(())
    }

    pub(in crate::filesystem) fn generation_ref_id(&self, id: GenerationId, io: Option<&PreparationIo>) -> Result<GenerationRef> {
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
        let generation = self
            .apply_generation(workspace, expected_generation, mutations, idempotency_key.clone(), None, None)
            .await?;
        self.generation_ref_id(generation.id(), None)
    }



    pub(in crate::filesystem) async fn apply_generation_guarded<F, Fut>(
        &self,
        workspace: &WorkspaceRef,
        expected_generation: Option<&GenerationRef>,
        mutations: &[WorkspaceMutation],
        idempotency_key: &IdempotencyKey,
        before_publication: &mut F,
    ) -> Result<Generation<A, O>>
    where
        F: FnMut() -> Fut + acyclic_stream::ProviderTask,
        Fut: std::future::Future<Output = Result<()>> + acyclic_stream::ProviderTask,
    {
        before_publication().await?;
        let workspace = self.open(workspace, None).await?;
        let key = filesystem_key(idempotency_key);
        let mut transaction = match expected_generation {
            Some(reference) => {
                let generation = self.generation(&workspace, reference, None).await?;
                workspace
                    .begin_transaction_at(&generation, key, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                    .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                    .map_err(map_error)?
            }
            None => workspace.begin_transaction(key, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error)?,
        };
        for mutation in mutations {
            before_publication().await?;
            match mutation {
                WorkspaceMutation::CreateDirectory { path } => {
                    transaction.create_dir_all(path, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error)?;
                }
                WorkspaceMutation::PutFile { path, bytes } => {
                    transaction
                        .write(path, Bytes::copy_from_slice(bytes), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                        .map_err(map_error)?;
                }
                WorkspaceMutation::Remove { path } => {
                    transaction.remove(path, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(map_error)?;
                }
            }
        }
        let cancellation = acyclic_fs::CancellationToken::new();
        match async { let guard = before_publication; transaction.commit(None, acyclic_fs::PublicationPermit::Unrestricted, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation, |_| { let future = guard(); async move { future.await.map(|value| acyclic_fs::OperationReceipt { value, work: acyclic_fs::WorkCounters::default() }).map_err(acyclic_fs::OperationFailure::before_work) } }).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }.await? {
            TransactionCommit::Committed(generation)
            | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
            TransactionCommit::Conflict { .. } => {
                Err(Error::Conflict("workspace generation changed".into()))
            }
            TransactionCommit::Fenced => Err(Error::Conflict("workspace writer was fenced".into())),
            TransactionCommit::IdempotencyConflict => Err(Error::Conflict(
                "filesystem idempotency key belongs to another mutation".into(),
            )),
        }
    }

    /// Resolves the current immutable head under the caller's work context.
    pub async fn resolve(&self, reference: &WorkspaceRef, io: Option<&PreparationIo>) -> Result<WorkspaceObservation> {
        let owned = if io.is_none() { Some(PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?) } else { None };
        let io = io.or(owned.as_ref()).expect("work context");
        let workspace = self.open(reference, Some(io)).await?;
        let head = io.sdk(workspace.head(io, io.cancellation()).await)?;
        let reference = crate::executor::with_history_io(Some(io), |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let resource = crate::core::copy_resource(reference.as_resource(), &mut quota)?;
            quota.transfer();
            drop(quota);
            WorkspaceRef::from_resource(resource, work)
        })?;
        Ok(WorkspaceObservation { workspace: reference, generation: self.generation_ref_id(head.id(), Some(io))? })
    }

    pub(in crate::filesystem) fn open<'a>(&'a self, reference: &'a WorkspaceRef, io: Option<&'a PreparationIo>) -> impl std::future::Future<Output=Result<Workspace<A,O>>> + acyclic_stream::ProviderTask + 'a {
        async move {
            let owned = if io.is_none() { Some(PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?) } else { None };
            let io = io.or(owned.as_ref()).expect("work context");
            reference.validate(&mut Some(&mut io.history_io()))?;
            if reference.as_resource().provider() != &self.provider { return Err(io.invalid("resource belongs to another Filesystem provider")); }
            let name = std::str::from_utf8(reference.as_resource().key()).map_err(|_| io.invalid("workspace reference key must be UTF-8"))?;
            io.sdk(self.filesystem.open_workspace(name, io, io.cancellation()).await)
        }
    }

    pub(in crate::filesystem) fn generation<'a>(&'a self, workspace: &'a Workspace<A,O>, reference: &'a GenerationRef, io: Option<&'a PreparationIo>) -> impl std::future::Future<Output=Result<Generation<A,O>>> + acyclic_stream::ProviderTask + 'a {
        async move {
            let owned = if io.is_none() { Some(PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?) } else { None };
            let io = io.or(owned.as_ref()).expect("work context");
            reference.validate(&mut Some(&mut io.history_io()))?;
            if reference.as_resource().provider() != &self.provider { return Err(io.invalid("resource belongs to another Filesystem provider")); }
            let key = reference.as_resource().key().try_into().map_err(|_| io.invalid("generation reference key must be 32 bytes"))?;
            io.sdk(workspace.generation(GenerationId::new(Digest::from_bytes(key)), io, io.cancellation()).await)
        }
    }

    pub(in crate::filesystem) fn apply_generation<'a>(
        &'a self, reference: &'a WorkspaceRef, expected: Option<&'a GenerationRef>, mutations: &'a [WorkspaceMutation],
        key: IdempotencyKey, io: Option<&'a PreparationIo>, preparation: Option<&'a crate::resource_preparation::TaskResourcePreparation>,
    ) -> impl std::future::Future<Output = Result<Generation<A,O>>> + acyclic_stream::ProviderTask + 'a {
        async move {
        let Some(io) = io else {
            return self.apply_generation_guarded(reference, expected, mutations, &key, &mut || std::future::ready(Ok(()))).await;
        };
        #[cfg(not(target_arch="wasm32"))]
        let mut native_before = {
            let owner = preparation.map(|task| Arc::clone(&task.owner));
            let expected = preparation.and_then(|task| task.native_custody.as_ref()).map(|custody| custody.work().clone());
            move |work: &acyclic_fs::NativeMaterializationWork| {
                let owner = owner.clone(); let expected = expected.clone(); let work = work.clone();
                async move {
                    match owner {
                        Some(owner) => {
                            if expected.is_none_or(|expected| !expected.shares_allocation_context(&work)) {
                                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized, reason: "preparation lacks original native custody" });
                            }
                            owner.verify_lease(false, Some(&PreparationIo::from_native_work(&work)?))?.await
                        },
                        None => Ok(()),
                    }
                }
            }
        };
        #[cfg(not(target_arch="wasm32"))]
        if let Some(work) = io.original_native_work() { native_before(work).await?; }
        let workspace = self.open(reference, Some(io)).await?;
        let pinned = match expected {
            Some(reference) => self.generation(&workspace, reference, Some(io)).await?,
            None => io.sdk(workspace.head(io, io.cancellation()).await)?,
        };
        let operation = fork_preparer_io::operation_key(&key, Some(io))?;
        let mut transaction = io.sdk(workspace.begin_pinned_transaction(&pinned, operation, io, io.cancellation()).await)?;
        for mutation in mutations {
            #[cfg(not(target_arch="wasm32"))]
            if let Some(work) = io.original_native_work() { native_before(work).await?; }
            match mutation {
                WorkspaceMutation::CreateDirectory { path } => io.sdk(transaction.create_dir_all(path, io, io.cancellation()).await)?,
                WorkspaceMutation::PutFile { path, bytes } => io.sdk(transaction.write(path, bytes.clone(), io, io.cancellation()).await)?,
                WorkspaceMutation::Remove { .. } => return Err(stock_boundary(Some(io), crate::wire::ErrorCode::Unsupported, "bounded publication does not remove content")),
            }
        }
        let commit = 'commit: {
            #[cfg(not(target_arch="wasm32"))]
            if let Some(work) = io.original_native_work() {
                break 'commit transaction.commit_with_native_work(None, acyclic_fs::PublicationPermit::Unrestricted, work, io.cancellation(), &mut native_before).await?;
            }
            io.receive(transaction.commit(None, acyclic_fs::PublicationPermit::Unrestricted, io.remaining()?, io.cancellation(), &mut |_| async move {
                let result = match preparation {
                    Some(task) => task.verify(false, Some(io)).map_err(acyclic_fs::OperationFailure::before_work)?.await,
                    None => Ok(()),
                };
                result.map(|value| acyclic_fs::OperationReceipt { value, work: acyclic_fs::WorkCounters::default() }).map_err(acyclic_fs::OperationFailure::before_work)
            }).await)?
        };
        match commit {
            TransactionCommit::Committed(generation) | TransactionCommit::AlreadyCommitted(generation) => Ok(generation),
            TransactionCommit::Conflict { .. } | TransactionCommit::Fenced | TransactionCommit::IdempotencyConflict =>
                Err(stock_boundary(Some(io), crate::wire::ErrorCode::Conflict, "workspace publication conflicted or was fenced")),
        }
        }
    }
    pub(in crate::filesystem) async fn operation_generation(&self, reference: &WorkspaceRef, key: &IdempotencyKey, io: Option<&PreparationIo>) -> Result<Option<Generation<A,O>>> {
        let owned = if io.is_none() { Some(PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?) } else { None };
        let io = io.or(owned.as_ref()).expect("work context");
        let workspace = self.open(reference, Some(io)).await?;
        let key = fork_preparer_io::operation_key(key, Some(io))?;
        io.sdk(workspace.operation_generation(key, io, io.cancellation()).await)
    }

}
