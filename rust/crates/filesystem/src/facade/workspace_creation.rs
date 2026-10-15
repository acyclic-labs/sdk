use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    /// Creates or recovers a named workspace under explicit work and cancellation.
    pub async fn create_workspace(
        &self,
        name: impl AsRef<str>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<crate::Workspace<A, O>>, WorkspaceError> {
        let lifecycle = if self.inner.capabilities.durable {
            Lifecycle::Durable
        } else {
            Lifecycle::Ephemeral
        };
        self.create_workspace_with_config(
            name,
            VolumeConfig::portable(lifecycle),
            budget,
            cancellation,
        )
        .await
    }

    /// Creates or recovers exact named semantics within one cumulative budget.
    pub async fn create_workspace_with_config(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<crate::Workspace<A, O>>, WorkspaceError> {
        self.create_workspace_with_config_operation(name, config, None, budget, cancellation)
            .await
    }
    /// Creates or recovers the immutable initial generation under one guarded work context.
    pub async fn create_workspace_with_operation<W, G, E>(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig,
        operation_id: OperationId,
        budget: W,
        cancellation: &CancellationToken,
        guard: Option<&mut G>,
    ) -> W::Output<crate::Generation<A, O>, E>
    where
        W: FsWork,
        G: crate::AuthorityGuard,
        G::Error: Into<E>,
        E: From<FsError> + From<WorkspaceError> + Send,
    {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let (name, id) = self.workspace_name(name.as_ref(), &mut accounting, cancellation)?;
            validate_volume_capabilities(config, self.inner.capabilities)?;
            let observed = self
                .read_creation(id.volume_id(), &mut accounting, cancellation)
                .await
                .map_err(WorkspaceError::from);
            let (creation, operation) = match observed {
                Ok(record) => record,
                Err(WorkspaceError::Filesystem(FsError::Authority(
                    AuthorityStoreError::Missing,
                ))) => {
                    let (_, root) = self
                        .create_volume_operation(
                            id.volume_id(),
                            config,
                            Some(operation_id),
                            &mut accounting,
                            cancellation,
                            guard,
                        )
                        .await?;
                    let observed = self
                        .read_creation(id.volume_id(), &mut accounting, cancellation)
                        .await
                        .map_err(WorkspaceError::from)?;
                    if observed.0.initial_generation_root != root {
                        return Err(E::from(FsError::CreationRejected));
                    }
                    observed
                }
                Err(WorkspaceError::Filesystem(FsError::VolumeMismatch)) => {
                    return Err(E::from(FsError::CreationRejected));
                }
                Err(error) => return Err(error.into()),
            };
            authenticate_named_creation(
                &creation,
                operation,
                id.volume_id(),
                config,
                operation_id,
            )?;
            let workspace = crate::Workspace {
                name,
                id,
                volume: Volume {
                    fs: self.clone(),
                    id: id.volume_id(),
                    config,
                },
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            };
            self.initial_generation(
                workspace,
                creation.initial_generation_root,
                &mut accounting,
                cancellation,
            )
            .await
            .map_err(Into::into)
        }
        .await;
        budget.finish(result, &accounting)
    }

    pub(super) async fn initial_generation(
        &self,
        workspace: crate::Workspace<A, O>,
        generation_root: ObjectId,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<crate::Generation<A, O>, WorkspaceError> {
        let (root, allocation) = read_generation_root_with_accounting(
            &self.inner.objects,
            generation_root,
            workspace.volume.config,
            accounting,
            cancellation,
        )
        .await?;
        if root.volume_id != workspace.volume.id {
            return Err(FsError::VolumeMismatch.into());
        }
        let bytes = root_parent_bytes(&root)?;
        Owned::new(root, allocation, bytes).retire(accounting)?;
        Ok(crate::Generation {
            workspace,
            id: GenerationId::new(generation_root.digest),
            publication_closure: None,
        })
    }
    /// Observes one original creation without retrying its publication.
    pub async fn workspace_for_creation_operation<W: FsWork>(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig,
        operation_id: OperationId,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<crate::Generation<A, O>>, WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let result = async {
            let (name, id) = self.workspace_name(name.as_ref(), &mut accounting, cancellation)?;
            let (creation, operation) = match self
                .read_creation(id.volume_id(), &mut accounting, cancellation)
                .await
                .map_err(WorkspaceError::from)
            {
                Ok(record) => record,
                Err(WorkspaceError::Filesystem(FsError::Authority(
                    AuthorityStoreError::Missing,
                ))) => return Ok(None),
                Err(WorkspaceError::Filesystem(FsError::VolumeMismatch)) => {
                    return Err(FsError::CreationRejected.into());
                }
                Err(error) => return Err(error),
            };
            authenticate_named_creation(
                &creation,
                operation,
                id.volume_id(),
                config,
                operation_id,
            )?;
            validate_volume_capabilities(config, self.inner.capabilities)?;
            let workspace = crate::Workspace {
                name,
                id,
                volume: Volume {
                    fs: self.clone(),
                    id: id.volume_id(),
                    config,
                },
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            };
            self.initial_generation(
                workspace,
                creation.initial_generation_root,
                &mut accounting,
                cancellation,
            )
            .await
            .map(Some)
        }
        .await;
        budget.finish(result, &accounting)
    }

    pub(crate) async fn create_workspace_with_config_operation(
        &self,
        name: impl AsRef<str>,
        config: VolumeConfig,
        operation_id: Option<OperationId>,
        budget: WorkBudget,
        cancellation: &CancellationToken,
    ) -> MeasuredResult<OperationReceipt<crate::Workspace<A, O>>, WorkspaceError> {
        let mut accounting = KernelAccounting::standalone(budget, cancellation);
        let result = async {
            let (name, id) = self.workspace_name(name.as_ref(), &mut accounting, cancellation)?;
            let (volume, _) = self
                .create_volume_operation::<FsError, ()>(
                    id.volume_id(),
                    config,
                    operation_id,
                    &mut accounting,
                    cancellation,
                    None::<&mut ()>,
                )
                .await
                .map_err(WorkspaceError::from)?;
            volume
                .resolve_head_generation(&mut accounting, cancellation)
                .await?;
            Ok(crate::Workspace {
                name,
                id,
                volume,
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            })
        }
        .await;
        measured(result, &accounting)
    }

    /// Authenticates one named workspace and its current authority head.
    pub async fn open_workspace<W: FsWork>(
        &self,
        name: impl AsRef<str>,
        budget: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::Workspace<A, O>, WorkspaceError> {
        let mut accounting = budget.accounting(cancellation);
        let result = (async {
            let (name, id) = self.workspace_name(name.as_ref(), &mut accounting, cancellation)?;
            let (creation, _) = self
                .read_creation(id.volume_id(), &mut accounting, cancellation)
                .await
                .map_err(WorkspaceError::from)?;
            validate_volume_capabilities(creation.config, self.inner.capabilities)?;
            let volume = Volume {
                fs: self.clone(),
                id: id.volume_id(),
                config: creation.config,
            };
            self.workspace_head_state(&volume, &mut accounting, cancellation)
                .await?;
            Ok(crate::Workspace {
                name,
                id,
                volume,
                #[cfg(all(feature = "native-watch", not(target_arch = "wasm32")))]
                source: None,
            })
        })
        .await;
        budget.finish(result, &accounting)
    }
}

pub(super) fn authenticate_named_creation(
    creation: &VolumeCreated,
    operation: OperationId,
    volume_id: VolumeId,
    config: VolumeConfig,
    original_operation_id: OperationId,
) -> Result<(), FsError> {
    if operation != original_operation_id
        || creation.volume_id != volume_id
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
    let hashed_bytes = (first.payload.len() as u64)
        .checked_mul(2)
        .and_then(|bytes| bytes.checked_add(CREATION_FINGERPRINT_DOMAIN.len() as u64))
        .and_then(|bytes| {
            bytes.checked_add(crate::foundation::AUTHORITY_COMMIT_DIGEST_ENVELOPE_BYTES)
        })
        .ok_or(WorkError::Overflow)?;
    let hashing = WorkCounters {
        bytes_hashed: hashed_bytes,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(hashing)?;
    let fingerprint = creation_fingerprint(&first.payload);
    let digest = crate::foundation::authority_commit_digest(
        volume_authority_id(volume_id),
        first.epoch,
        first.sequence,
        first.operation_id,
        first.fingerprint,
        first.previous_digest,
        &first.payload,
    );
    accounting.record_completed(hashing)?;
    if first.fingerprint != fingerprint || first.digest != digest {
        return Err(FsError::CreationRejected);
    }
    let mut projection = crate::kernel::OriginalReadAccounting::new(accounting)?;
    let creation = crate::kernel::decode_volume_created::<FsError>(
        &first.payload,
        MAXIMUM_VOLUME_EVENT_BYTES,
        &mut |step| projection.observe(step),
    );
    projection.release_failed()?;
    let creation = creation?;
    if creation.volume_id != volume_id {
        return Err(FsError::VolumeMismatch);
    }
    Ok((creation, first.operation_id))
}
