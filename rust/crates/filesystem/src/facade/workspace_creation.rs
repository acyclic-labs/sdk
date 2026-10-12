use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {


    /// Creates or recovers a named workspace under explicit work and cancellation.
    pub async fn create_workspace(
        &self, name: impl AsRef<str>, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    {
        let lifecycle = if self.inner.capabilities.durable {
            Lifecycle::Durable
        } else {
            Lifecycle::Ephemeral
        };
        self.create_workspace_with_config(name, VolumeConfig::portable(lifecycle), budget, cancellation).await
    }



    /// Creates or recovers exact named semantics within one cumulative budget.
    pub async fn create_workspace_with_config(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig,
        budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    { self.create_workspace_with_config_operation(name, config, None, budget, cancellation) .await }


    /// Creates or recovers one original creation operation and its immutable
    pub async fn create_workspace_with_operation(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig, operation_id: OperationId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Generation<A, O>>, WorkspaceError>
    {

        let created = self.create_named_workspace(name, config, Some(operation_id), budget, cancellation).await?;
        let prior = created.work;
        let live = created.value.name.retained_bytes();
        let allowance = remaining_owned(prior, budget, live)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let observed = self.read_creation_record(created.value.volume.id, allowance, cancellation).await
            .map_err(|failure| {
                merge_simultaneous_failure(prior, *failure.work, live, failure.error)
                    .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
            })?;
        let creation = observed.value.0;
        let work = merge_simultaneous_work(prior, observed.work, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        self.initial_workspace_generation(
            created.value, creation.initial_generation_root, work, budget, cancellation,
        ).await
    }


    /// Observes one original workspace creation without retrying its publication.
    pub async fn workspace_for_creation_operation(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig, original_operation_id: OperationId, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<Option<crate::Generation<A, O>>>, WorkspaceError> {

        let named = self.workspace_name_identity(name, budget, cancellation)?;
        let (name, id) = named.value;
        let mut work = named.work;
        let live = name.retained_bytes();
        let allowance = remaining_owned(work, budget, live)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let (creation, operation, nested) = match self.read_creation_record(
            id.volume_id(), allowance, cancellation,
        ).await {
            Ok(observed) => (observed.value.0, observed.value.1, observed.work), Err(failure) => {
                let failure = merge_simultaneous_failure(work, *failure.work, live, failure.error);
                if matches!(&failure.error, FsError::Authority(AuthorityStoreError::Missing)) {
                    return Ok(FsReceipt { value: None, work: *failure.work });
                }
                return Err(failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from));
            }
        };
        work = merge_simultaneous_work(work, nested, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        authenticate_named_creation(
            &creation, operation, id.volume_id(), config, original_operation_id,
        ).map_err(|error| OperationFailure::new(WorkspaceError::from(error), work))?;
        validate_volume_capabilities(config, self.inner.capabilities)
            .map_err(|error| OperationFailure::new(WorkspaceError::from(error), work))?;
        let workspace = crate::Workspace {
            name, id, volume: Volume { fs: self.clone(), id: id.volume_id(), config },
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: None,
        };
        self.initial_workspace_generation(
            workspace, creation.initial_generation_root, work, budget, cancellation,
        ).await.map(|receipt| FsReceipt { value: Some(receipt.value), work: receipt.work })
    }


    /// Creates or recovers the immutable initial generation on the original native ledger.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn create_workspace_with_operation_with_native_work(
        &self,
        name: &str, config: VolumeConfig, original_operation_id: OperationId, original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, (WorkspaceError, Option<NativeAllocation>)> {
        let publication = |id, commit, _| async move {
            (self.inner.authority.create_authority_with_first_record_with_native_work(
                id, commit, original, cancellation,
            ).await.map_err(|error| WorkspaceError::from(FsError::from(error))), WorkCounters::UNCHARGED)
        };
        self.create_workspace_with_operation_with_native_work_value(
            name, config, original_operation_id, original, cancellation, publication,
        ).await.map_err(|error| (error, None))
    }


    /// Creates or recovers the immutable initial generation, sealing every real
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn create_workspace_with_operation_guarded_with_native_work<'original, E, F, Fut>(
        &self,
        name: &str,
        config: VolumeConfig,
        original_operation_id: OperationId, original: &'original NativeMaterializationWork,
        cancellation: &CancellationToken,
        before_publication: &mut F,
    ) -> Result<crate::Generation<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send + 'static,
        F: for<'m> FnMut(&'m NativeMaterializationWork) -> Fut + Clone + Send + 'static,
        Fut: Future<Output = Result<(), E>> + Send + 'static,
    {
        let publication = |id, commit, _| async move {
            (self.inner.authority.create_authority_with_first_record_guarded_with_native_work(
                id, commit, original, cancellation, before_publication,
            ).await, WorkCounters::UNCHARGED)
        };
        self.create_workspace_with_operation_with_native_work_value(
            name, config, original_operation_id, original, cancellation, publication,
        ).await
    }


    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn create_workspace_with_operation_with_native_work_value<E, P, Fut>(
        &self, name: &str, config: VolumeConfig, original_operation_id: OperationId,
        original: &NativeMaterializationWork, cancellation: &CancellationToken, publication: P,
    ) -> Result<crate::Generation<A, O>, E>
    where
        E: From<FsError> + From<WorkspaceError> + Send,
        P: FnOnce(crate::AuthorityId, ProposedCommit, WorkBudget) -> Fut + ProviderTask,
        Fut: Future<Output = (Result<bool, E>, WorkCounters)> + ProviderTask,
    {

        cancellation.check().map_err(FsError::from)?;
        original.admit(WorkCounters::UNCHARGED).map_err(FsError::from)?;
        let name = crate::WorkspaceName::new(name, original, cancellation)?;
        let id = crate::WorkspaceId::derive(
            self.inner.workspace_namespace, &name, original, cancellation,
        )?;
        validate_volume_capabilities(config, self.inner.capabilities)?;
        let observed = self.read_creation_record(id.volume_id(), original, cancellation).await;
        let (creation, operation) = match observed {
            Ok(creation) => creation, Err(FsError::Authority(AuthorityStoreError::Missing)) => {
                let mut accounting = KernelAccounting::native(original, cancellation);
                let (_, initial_root) = self.create_volume_with_id_operation_with_accounting(
                    id.volume_id(), config, Some(original_operation_id), &mut accounting, cancellation, publication,
                ).await?;
                let observed = self.read_creation_record(
                    id.volume_id(), original, cancellation,
                ).await.map_err(|error| match error {
                    FsError::VolumeMismatch => WorkspaceError::from(FsError::CreationRejected), error => WorkspaceError::from(error),
                })?;
                if observed.0.initial_generation_root != initial_root {
                    return Err(WorkspaceError::from(FsError::CreationRejected).into());
                }
                observed
            }
            Err(FsError::VolumeMismatch) => return Err(WorkspaceError::from(FsError::CreationRejected).into()),
            Err(error) => return Err(WorkspaceError::from(error).into()),
        };
        authenticate_named_creation(&creation, operation, id.volume_id(), config, original_operation_id)?;
        let workspace = crate::Workspace {
            name, id, volume: Volume { fs: self.clone(), id: id.volume_id(), config },
            #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
            source: None,
        };
        self.initial_workspace_generation_with_native_work(
            workspace, creation.initial_generation_root, original, cancellation,
        ).await.map_err(Into::into)
    }


    /// Authenticates an original creation without publishing or reading its head.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn workspace_for_creation_operation_with_native_work(
        &self, name: &str, config: VolumeConfig, original_operation_id: OperationId,
        original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<Option<crate::Generation<A, O>>, (WorkspaceError, Option<NativeAllocation>)> {
        let result: Result<_, WorkspaceError> = async {
            cancellation.check()?;
            original.admit(WorkCounters::UNCHARGED).map_err(FsError::from)?;
            let name = crate::WorkspaceName::new(name, original, cancellation)?;
            let id = crate::WorkspaceId::derive(
                self.inner.workspace_namespace, &name, original, cancellation,
            )?;
            let (creation, operation) = match self.read_creation_record(
                id.volume_id(), original, cancellation,
            ).await {
                Ok(creation) => creation, Err(FsError::Authority(AuthorityStoreError::Missing)) => return Ok(None),
                Err(FsError::VolumeMismatch) => return Err(WorkspaceError::from(FsError::CreationRejected)),
                Err(error) => return Err(WorkspaceError::from(error)),
            };
            authenticate_named_creation(&creation, operation, id.volume_id(), config, original_operation_id)?;
            validate_volume_capabilities(config, self.inner.capabilities)?;
            let workspace = crate::Workspace {
                name, id, volume: Volume { fs: self.clone(), id: id.volume_id(), config },
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            };
            self.initial_workspace_generation_with_native_work(
                workspace, creation.initial_generation_root, original, cancellation,
            ).await.map(Some)
        }.await;
        result.map_err(|error| (error, None))
    }


    /// Opens the workspace authenticated by its immutable native creation record.
    #[cfg(not(target_arch = "wasm32"))]
    pub async fn open_workspace_with_native_work(
        &self, name: &str, original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<crate::Workspace<A, O>, (WorkspaceError, Option<NativeAllocation>)> {
        let result: Result<_, WorkspaceError> = async {
            cancellation.check()?;
            original.admit(WorkCounters::UNCHARGED).map_err(FsError::from)?;
            let name = crate::WorkspaceName::new(name, original, cancellation)?;
            let id = crate::WorkspaceId::derive(
                self.inner.workspace_namespace, &name, original, cancellation,
            )?;
            let (creation, _) = self.read_creation_record(id.volume_id(), original, cancellation).await?;
            validate_volume_capabilities(creation.config, self.inner.capabilities)?;
            Ok(crate::Workspace {
                name, id, volume: Volume { fs: self.clone(), id: id.volume_id(), config: creation.config },
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            })
        }.await;
        result.map_err(|error| (error, None))
    }


    #[cfg(not(target_arch = "wasm32"))]
    pub(super) async fn initial_workspace_generation_with_native_work(
        &self, workspace: crate::Workspace<A, O>, generation_root: ObjectId, original: &NativeMaterializationWork,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, WorkspaceError> {
        let checkout = workspace.volume.checkout_with_native_work(
            GenerationSelector::Exact(GenerationId::new(generation_root.digest)), CheckoutMode::read_only_pinned(), original, cancellation,
        ).await?;
        Ok(crate::Generation { workspace, id: checkout.generation_id(), publication_closure: None, })
    }


    pub(super) async fn initial_workspace_generation(
        &self, workspace: crate::Workspace<A, O>, generation_root: ObjectId, work: WorkCounters, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Generation<A, O>>, WorkspaceError> {

        let live = workspace.name.retained_bytes();
        let allowance = remaining_owned(work, budget, live)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let checkout = workspace.volume.checkout(
            GenerationSelector::Exact(GenerationId::new(generation_root.digest)), CheckoutMode::read_only_pinned(), allowance, cancellation,
        ).await.map_err(|failure| {
            merge_simultaneous_failure(work, *failure.work, live, failure.error)
                .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
        })?;
        let work = merge_simultaneous_work(work, checkout.work, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        Ok(FsReceipt {
            value: crate::Generation { workspace, id: checkout.value.generation_id(), publication_closure: None, }, work,
        })
    }


    pub(crate) async fn create_workspace_with_config_operation(
        &self, name: impl AsRef<str>, config: VolumeConfig, operation_id: Option<OperationId>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    {
        let span = crate::obs::span!(
            INFO, "acyclic.fs.create_workspace", outcome = crate::obs::Empty, error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty, work.bytes = crate::obs::Empty, work.durability = crate::obs::Empty
        );

        crate::obs::in_span(&span, async move {
            let created = self.create_named_workspace(name, config, operation_id, budget, cancellation).await?;
            let work = created.work;
            let live = created.value.name.retained_bytes();
            let allowance = remaining_owned(work, budget, live)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            let head = created.value.volume.resolve_head_generation(allowance, cancellation).await.map_err(|failure| {
                    merge_simultaneous_failure(work, *failure.work, live, failure.error)
                        .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
                })?;
            let work = merge_simultaneous_work(work, head.2, live, budget)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            Ok(FsReceipt { value: created.value, work, })
        }).await.observe_on(&span, "create_workspace")
    }


    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn workspace_name_identity_with_native_work(
        &self, name: impl AsRef<str>, original: &NativeMaterializationWork, cancellation: &CancellationToken,
    ) -> Result<(crate::WorkspaceName, crate::WorkspaceId), (WorkspaceError, Option<NativeAllocation>)> {
        let result = (|| {
            let name = crate::WorkspaceName::new(name.as_ref(), original, cancellation)?;
            let id = crate::WorkspaceId::derive(
                self.inner.workspace_namespace, &name, original, cancellation,
            )?;
            Ok((name, id))
        })();
        result.map_err(|error| (error, None))
    }

    pub(super) async fn create_named_workspace(
        &self, name: impl AsRef<str>, config: VolumeConfig, operation_id: Option<OperationId>, budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    {

        let named = self.workspace_name_identity(name, budget, cancellation)?;
        let (name, id) = named.value;
        let prior = named.work;
        let live = name.retained_bytes();
        let allowance = remaining_owned(prior, budget, live)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        let created = self.create_volume_with_id_operation(id.volume_id(), config, operation_id, allowance, cancellation).await
            .map_err(|failure| {
                merge_simultaneous_failure(prior, *failure.work, live, failure.error)
                    .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
            })?;
        let work = merge_simultaneous_work(prior, created.work, live, budget)
            .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
        Ok(FsReceipt {
            value: crate::Workspace {
                name, id, volume: created.value,
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            }, work,
        })
    }



    /// Authenticates named workspace creation and its current head under one
    pub async fn open_workspace(
        &self, name: impl AsRef<str>, budget: WorkBudget, cancellation: &CancellationToken,
    ) -> MeasuredResult<FsReceipt<crate::Workspace<A, O>>, WorkspaceError>
    {
        let span = crate::obs::span!(
            INFO, "acyclic.fs.open_workspace", outcome = crate::obs::Empty, error.kind = crate::obs::Empty,
            work.items = crate::obs::Empty, work.bytes = crate::obs::Empty, work.durability = crate::obs::Empty
        );

        crate::obs::in_span(&span, async move {
            let named = self.workspace_name_identity(name, budget, cancellation)?;
            let (name, id) = named.value;
            let prior = named.work;
            let live = name.retained_bytes();
            let allowance = remaining_owned(prior, budget, live)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            let opened = self.open_volume(id.volume_id(), allowance, cancellation).await.map_err(|failure| {
                    merge_simultaneous_failure(prior, *failure.work, live, failure.error)
                        .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
                })?;
            let work = merge_simultaneous_work(prior, opened.work, live, budget)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            let allowance = remaining_owned(work, budget, live)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            let head = opened.value.resolve_head_generation(allowance, cancellation).await.map_err(|failure| {
                    merge_simultaneous_failure(work, *failure.work, live, failure.error)
                        .map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from)
                })?;
            let work = merge_simultaneous_work(work, head.2, live, budget)
                .map_err(|failure| failure.map_with_prior_work(WorkCounters::UNCHARGED, WorkspaceError::from))?;
            Ok(FsReceipt {
                value: crate::Workspace {
                    name, id, volume: opened.value,
                    #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                    source: None,
                }, work,
            })
        }).await.observe_on(&span, "open_workspace")
    }
}

pub(super) fn authenticate_named_creation(
    creation: &VolumeCreated,
    operation: OperationId,
    volume_id: VolumeId,
    config: VolumeConfig,
    original_operation_id: OperationId,
) -> Result<(), FsError> {
    if operation != original_operation_id || creation.volume_id != volume_id
        || creation.config != config
    {
        return Err(FsError::CreationRejected);
    }
    Ok(())
}

pub(super) fn decode_first_creation_record(
    volume_id: VolumeId,
    records: &[crate::foundation::DurableCommit],
    accounting: &mut crate::kernel::allocation::KernelAccounting<'_>,
) -> Result<(VolumeCreated, OperationId), FsError> {
    accounting.check()?;
    let first = records.first().ok_or(FsError::EmptyAuthority)?;
    if first.sequence != Sequence::new(1) || first.previous_digest != Digest::ZERO {
        return Err(FsError::CreationRejected);
    }
    let hashed_bytes = (first.payload.len() as u64).checked_mul(2)
        .and_then(|bytes| bytes.checked_add(CREATION_FINGERPRINT_DOMAIN.len() as u64))
        .and_then(|bytes| bytes.checked_add(crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES))
        .ok_or(WorkError::Overflow)?;
    let hashing = WorkCounters { bytes_hashed: hashed_bytes, ..WorkCounters::UNCHARGED };
    accounting.admit(hashing)?;
    let fingerprint = creation_fingerprint(&first.payload);
    let digest = crate::foundation::authority_commit_digest(
        volume_authority_id(volume_id), first.epoch, first.sequence, first.operation_id,
        first.fingerprint, first.previous_digest, &first.payload,
    );
    accounting.record_completed(hashing)?;
    if first.fingerprint != fingerprint || first.digest != digest {
        return Err(FsError::CreationRejected);
    }
    let mut projection = crate::kernel::OriginalReadAccounting::new(accounting)?;
    let creation = crate::kernel::decode_volume_created::<FsError>(
        &first.payload, MAXIMUM_VOLUME_EVENT_BYTES, &mut |step| projection.observe(step),
    );
    projection.release_failed()?;
    let creation = creation?;
    if creation.volume_id != volume_id {
        return Err(FsError::VolumeMismatch);
    }
    Ok((creation, first.operation_id))
}

