use super::*;

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
        #[cfg(all(test, not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record(("read", maximum_bytes));
        let owned;
        let io = if let Some(io) = io {
            io
        } else {
            owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
            &owned
        };
        let workspace = self.open(reference, Some(io)).await?;
        let bytes = match generation {
            Some(reference) => {
                let pinned = self.generation(&workspace, reference, Some(io)).await?;
                crate::conversation::BoundedContentFuture::new(Some(io), || {
                    pinned
                        .read(path, maximum_bytes, io, io.cancellation())
                        .map(|result| io.sdk(result))
                })?
                .await?
            }
            None => {
                crate::conversation::BoundedContentFuture::new(Some(io), || {
                    workspace
                        .read(path, maximum_bytes, io, io.cancellation())
                        .map(|result| io.sdk(result))
                })?
                .await?
            }
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
        #[cfg(all(test, not(target_arch = "wasm32")))]
        acyclic_test_support::differential::record(("retain", 0u64));
        let workspace = self.open(workspace, None).await?;
        let generation = self.generation(&workspace, reference, None).await?;
        let identity = format!(
            "harness-generation-{}",
            hex::encode(reference.as_resource().key())
        );
        generation
            .pin(
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
        for mutation in mutations {
            match mutation {
                WorkspaceMutation::CreateDirectory { path } => {
                    transaction
                        .create_dir_all(path, budget, &cancellation)
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)
                        .map_err(map_error)?;
                }
                WorkspaceMutation::PutFile { path, bytes } => {
                    transaction
                        .write(path, bytes.clone(), budget, &cancellation)
                        .await
                        .map(|receipt| receipt.value)
                        .map_err(|failure| failure.error)
                        .map_err(map_error)?;
                }
                WorkspaceMutation::Remove { path } => {
                    transaction
                        .remove(path, budget, &cancellation)
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
                && let WorkspaceMutation::PutFile { path, .. } | WorkspaceMutation::Remove { path } =
                    mutation
            {
                let metadata = content_metadata_path_io(path.trim_start_matches('/'), true, None)?;
                match transaction.remove(&metadata, budget, &cancellation).await {
                    Ok(_) => {}
                    Err(failure) if matches!(failure.error, WorkspaceError::NotFound) => {}
                    Err(failure) => return Err(map_error(failure.error)),
                }
            }
        }
        loop {
            let committed = transaction
                .commit(
                    None,
                    acyclic_fs::PublicationPermit::Unrestricted,
                    budget,
                    &cancellation,
                    |_| async {
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
                TransactionCommit::Conflict { .. } if expected_generation.is_none() => {
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
        let unmeasured = io.is_none();
        crate::conversation::BoundedContentFuture::new(io, || async move {
            let owned;
            let io = match io {
                Some(io) => io,
                None => {
                    owned = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
                    &owned
                }
            };
            let workspace = crate::conversation::BoundedContentFuture::new(Some(io), || {
                self.open(reference, Some(io))
            })?
            .await?;
            let mut publication = publication::Update {
                host: self,
                workspace: &workspace,
                expected,
                mutations,
                operation: fork_preparer_io::operation_key(&key, Some(io))?,
                io,
                preparation,
                unmeasured,
            };
            publication::finish(&mut publication, publication::attempt).await
        })
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod bytes_tests;
mod publication;
