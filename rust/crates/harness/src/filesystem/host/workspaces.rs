use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Resolves the current immutable head of a named workspace.
    pub async fn resolve(&self, reference: &WorkspaceRef) -> Result<WorkspaceObservation> {
        let workspace = self.open(reference).await?;
        let head = workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?;
        Ok(WorkspaceObservation {
            workspace: reference.clone(),
            generation: self.generation_ref_id(head.id())?,
        })
    }

    /// Reads one bounded file from the current head or an exact generation.
    pub async fn read(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
        maximum_bytes: u64,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?
                .read(path, maximum_bytes, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                .await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error)),
            None => workspace.read(path, maximum_bytes, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error)),
        }
    }

    /// Stats one path at the current head or an exact generation.
    pub async fn stat(
        &self,
        workspace: &WorkspaceRef,
        generation: Option<&GenerationRef>,
        path: &str,
    ) -> Result<WorkspaceStat> {
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
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
        let workspace = self.open(workspace).await?;
        match generation {
            Some(reference) => self
                .generation(&workspace, reference)
                .await?
                .list_directory(path, after, maximum_entries, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                .await.map(|receipt| receipt.value.0).map_err(|failure| map_error(failure.error)),
            None => workspace
                .list_directory(path, after, maximum_entries, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                .await.map(|receipt| receipt.value.0).map_err(|failure| map_error(failure.error)),
        }
    }

    pub(in crate::filesystem) async fn open(&self, reference: &WorkspaceRef) -> Result<Workspace<A, O>> {
        reference.validate(&mut None)?;
        self.validate_provider(reference.as_resource().provider())?;
        let name = std::str::from_utf8(reference.as_resource().key())
            .map_err(|_| Error::Invalid("workspace reference key must be UTF-8".into()))?;
        self.filesystem
            .open_workspace(name, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))
    }

    pub(in crate::filesystem) async fn generation(
        &self,
        workspace: &Workspace<A, O>,
        reference: &GenerationRef,
    ) -> Result<Generation<A, O>> {
        reference.validate(&mut None)?;
        self.validate_provider(reference.as_resource().provider())?;
        let bytes: [u8; 32] = reference
            .as_resource()
            .key()
            .try_into()
            .map_err(|_| Error::Invalid("generation reference key must be 32 bytes".into()))?;
        workspace
            .generation(GenerationId::new(Digest::from_bytes(bytes)), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))
    }

    pub(in crate::filesystem) async fn retain_generation(
        &self,
        workspace: &WorkspaceRef,
        reference: &GenerationRef,
    ) -> Result<()> {
        let workspace = self.open(workspace).await?;
        let generation = self.generation(&workspace, reference).await?;
        let identity = format!(
            "harness-generation-{}",
            hex::encode(reference.as_resource().key())
        );
        generation.pin(identity, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?;
        Ok(())
    }

    pub(in crate::filesystem) fn generation_ref_id(&self, id: GenerationId) -> Result<GenerationRef> {
        GenerationRef::new(self.provider.clone(), id.digest().into_bytes().into(), None)
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
            .apply_generation(workspace, expected_generation, mutations, idempotency_key)
            .await?;
        self.generation_ref_id(generation.id())
    }

    pub(in crate::filesystem) async fn apply_generation(
        &self,
        workspace: &WorkspaceRef,
        expected_generation: Option<&GenerationRef>,
        mutations: &[WorkspaceMutation],
        idempotency_key: &IdempotencyKey,
    ) -> Result<Generation<A, O>> {
        let mut before_publication = || std::future::ready(Ok(()));
        self.apply_generation_guarded(
            workspace, expected_generation, mutations, idempotency_key,
            &mut before_publication,
        ).await
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
        let workspace = self.open(workspace).await?;
        let key = filesystem_key(idempotency_key);
        let mut transaction = match expected_generation {
            Some(reference) => {
                let generation = self.generation(&workspace, reference).await?;
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
        match async { let mut guard = before_publication; transaction.commit(None, acyclic_fs::PublicationPermit::Unrestricted, acyclic_fs::WorkBudget::UNBOUNDED, &cancellation, |_| { let future = guard(); async move { future.await.map(|value| acyclic_fs::OperationReceipt { value, work: acyclic_fs::WorkCounters::default() }).map_err(acyclic_fs::OperationFailure::before_work) } }).await.map(|receipt| receipt.value).map_err(|failure| failure.error) }.await? {
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

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn open_volume_with_native_work(
        &self,
        volume: &VolumeRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Workspace<A, O>> {
        if volume.provider() != &self.provider {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "volume belongs to another Filesystem provider",
            });
        }
        let io = PreparationIo::from_native_work(work)?;
        let reference = fork_preparer::bounded_io::content_workspace(&self.provider, volume, Some(&io))?;
        fork_preparer::bounded_io::open(self, &reference, &io).await
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) async fn generation_with_native_work(
        &self,
        workspace: &Workspace<A, O>,
        reference: &GenerationRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Generation<A, O>> {
        let io = PreparationIo::from_native_work(work)?;
        fork_preparer::bounded_io::generation(self, workspace, reference, &io).await
    }
}
