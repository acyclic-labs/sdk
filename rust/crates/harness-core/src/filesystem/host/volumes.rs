use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Forks selected volume paths at an exact generation.
    pub async fn fork_volume(
        &self,
        source: &VolumeRef,
        source_grant: &ContentGrant,
        source_generation: &GenerationRef,
        destination: &VolumeRef,
        destination_grant: &ContentGrant,
        paths: Option<Vec<String>>,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation>
    where
        A: 'static,
    {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        if crate::core::filesystem_scope_label(
            destination_grant.policy_scope().capabilities(),
            &mut None,
        )?
        .is_some()
        {
            return Err(Error::FilesystemAccessConfiguration(
                "structured fork destination requires an existing policy view",
            ));
        }
        self.fork_volume_pinned(
            source,
            source_grant,
            source_generation,
            destination,
            paths,
            idempotency_key,
            None,
            None,
        )
        .await
    }

    pub(in crate::filesystem) async fn fork_volume_pinned(
        &self,
        source: &VolumeRef,
        grant: &ContentGrant,
        selected: &GenerationRef,
        child: &VolumeRef,
        paths: Option<Vec<String>>,
        key: &IdempotencyKey,
        io: Option<&PreparationIo>,
        preparation: Option<&Arc<crate::resource_preparation::TaskResourcePreparation>>,
    ) -> Result<WorkspaceObservation>
    where
        A: 'static,
    {
        let host = self;

        if io.is_some_and(|io| io.original_native_work().is_some()) && preparation.is_none() {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "task-bound native fork requires its original issued creation guard",
            ));
        }
        if let Some(task) = preparation {
            task.verify(false, io)?.await?;
        }
        grant.require(source, VolumeOperation::Read, io)?;
        let owned = if io.is_none() {
            Some(PreparationIo::for_unmeasured(
                &acyclic_fs::CancellationToken::new(),
            )?)
        } else {
            None
        };
        let io = io.or(owned.as_ref()).ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "volume fork has no work context",
            )
        })?;
        validate_fork_volumes(source, child, &host.provider, io)?;
        let reference = bounded_io::content_workspace(&host.provider, source, Some(io))?;
        let workspace = host.open(&reference, Some(io)).await?;
        let pinned = host.generation(&workspace, selected, Some(io)).await?;
        bounded_io::retain(&pinned, Some(io)).await?;
        let name = crate::executor::with_history_io(Some(io), |work| child.storage_name(work))?;
        let operation = bounded_io::operation_key(key, Some(io))?;
        let forked = io.sdk(
            workspace
                .with_stable_policy(io, io.cancellation(), || async {
                    super::volume_access::authorize_fork(&workspace, grant, pinned.id(), io)
                        .await?;
                    let options = acyclic_fs::ForkOptions::from_generation(pinned, operation);
                    let options = match paths {
                        Some(paths) => options.inherit_paths(paths),
                        None => options,
                    };
                    let forked = match preparation {
                        Some(task) => io.sdk(
                            workspace
                                .fork::<_, _, Error>(
                                    &name,
                                    options,
                                    acyclic_fs::PublicationPermit::Unrestricted,
                                    io,
                                    io.cancellation(),
                                    task.as_ref(),
                                )
                                .await,
                        )?,
                        None => io.sdk(
                            workspace
                                .fork::<_, _, Error>(
                                    &name,
                                    options,
                                    acyclic_fs::PublicationPermit::Unrestricted,
                                    io,
                                    io.cancellation(),
                                    &mut |_| {
                                        std::future::ready(Ok(acyclic_fs::OperationReceipt {
                                            value: (),
                                            work: WorkCounters::default(),
                                        }))
                                    },
                                )
                                .await,
                        )?,
                    };
                    Ok::<_, Error>(forked)
                })
                .await,
        )??;
        io.remaining()?;
        let observed = host.fork_observation(&forked, name, operation, io).await?;
        if io.original_native_work().is_none()
            && let Some(task) = preparation
        {
            task.verify(false, Some(io))?.await?;
        }
        Ok(observed)
    }

    async fn fork_observation(
        &self,
        forked: &acyclic_fs::Workspace<A, O>,
        name: String,
        operation: acyclic_fs::IdempotencyKey,
        io: &PreparationIo,
    ) -> Result<WorkspaceObservation> {
        let initial = io
            .sdk(
                forked
                    .operation_generation(operation, io, io.cancellation())
                    .await,
            )?
            .ok_or_else(|| {
                stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Storage,
                    "fork creation has no durable original generation",
                )
            })?;
        bounded_io::retain(&initial, Some(io)).await?;
        let observed = WorkspaceObservation {
            workspace: WorkspaceRef::new(
                self.provider.clone_with_io(io)?,
                name.into_bytes(),
                None,
            )?,
            generation: bounded_io::generation_ref(&self.provider, initial.id(), Some(io))?,
        };
        Ok(observed)
    }

    /// Plans a destination-authorized volume import.
    pub async fn prepare_volume_import<'a>(
        &self,
        source: &VolumeRef,
        source_grant: &'a ContentGrant,
        generation: &GenerationRef,
        destination: &VolumeRef,
        destination_grant: &'a ContentGrant,
    ) -> Result<VolumeImportPlan<'a, A, O>> {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        source.validate(&mut None)?;
        destination.validate(&mut None)?;
        self.validate_provider(source.provider())?;
        self.validate_provider(destination.provider())?;
        let source_workspace = self
            .open(
                &workspace_ref(self.provider.clone(), &source.storage_name(&mut None)?)?,
                None,
            )
            .await?;
        let destination_workspace = self
            .open(
                &workspace_ref(self.provider.clone(), &destination.storage_name(&mut None)?)?,
                None,
            )
            .await?;
        let source_generation = self.generation(&source_workspace, generation, None).await?;
        let target = destination_workspace
            .head(
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?;
        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let source_access = super::volume_access::compile(
            &source_workspace,
            source_grant,
            Some(source_generation.id()),
            VolumeOperation::Read,
            &[
                acyclic_fs::access::Action::Read,
                acyclic_fs::access::Action::List,
            ],
            &io,
        )
        .await?;
        let destination_access = super::volume_access::compile(
            &destination_workspace,
            destination_grant,
            None,
            VolumeOperation::Write,
            &[acyclic_fs::access::Action::MergeInto],
            &io,
        )
        .await?;
        let plan = source_workspace
            .join_into(&destination_workspace)
            .plan_at(
                &source_generation,
                &target,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map(|receipt| receipt.value)
            .map_err(|failure| map_error(failure.error))?;
        super::super::access::require_current(&source_workspace, &source_access.0, &io).await?;
        super::super::access::require_current(&destination_workspace, &destination_access.0, &io)
            .await?;
        Ok(VolumeImportPlan::new(plan, source_grant, destination_grant))
    }

    /// Configuration and initial access policy of every harness volume.
    pub(in crate::filesystem) fn volume_initialization(
        &self,
    ) -> acyclic_fs::VolumeInitialization<'static> {
        acyclic_fs::VolumeInitialization {
            config: acyclic_fs::VolumeConfig::portable(if self.filesystem.capabilities().durable {
                acyclic_fs::Lifecycle::Durable
            } else {
                acyclic_fs::Lifecycle::Ephemeral
            }),
            access_policy: Some(access::INITIAL_POLICY),
        }
    }

    /// Creates the unique physical workspace for a logical volume.
    pub async fn create_volume(&self, volume: &VolumeRef) -> Result<WorkspaceObservation> {
        volume.validate(&mut None)?;
        if volume.provider() != &self.provider {
            return Err(Error::Invalid(
                "volume belongs to another Filesystem provider".into(),
            ));
        }
        let name = volume.storage_name(&mut None)?;
        let digest =
            blake3::derive_key("acyclic harness volume initialization v1", name.as_bytes());
        let mut operation = [0; 16];
        operation.copy_from_slice(&digest[..16]);
        self.filesystem
            .create_workspace_with_operation::<_, (), WorkspaceError>(
                &name,
                self.volume_initialization(),
                acyclic_fs::OperationId::from_bytes(operation),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
                None,
            )
            .await
            .map_err(|failure| map_error(failure.error))?;
        self.resolve(&workspace_ref(self.provider.clone(), &name)?, None)
            .await
    }
}

fn validate_fork_volumes(
    source: &VolumeRef,
    child: &VolumeRef,
    provider: &ProviderRef,
    io: &PreparationIo,
) -> Result<()> {
    source.validate(&mut Some(&mut io.history_io()))?;
    child.validate(&mut Some(&mut io.history_io()))?;
    if source.provider() != provider || child.provider() != provider || source == child {
        return Err(io.invalid("volume fork has a foreign or identical destination"));
    }
    Ok(())
}
