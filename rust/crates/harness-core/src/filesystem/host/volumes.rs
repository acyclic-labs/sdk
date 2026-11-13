use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// The immutable generation created by this exact volume operation, if
    /// committed. It remains the diff base after the current head changes.
    pub async fn creation_generation(
        &self,
        volume: &VolumeRef,
        operation: acyclic_fs::OperationId,
    ) -> Result<Option<GenerationRef>> {
        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let reference = bounded_io::content_workspace(&self.provider, volume, Some(&io))?;
        let workspace = self.open(&reference, Some(&io)).await?;
        let created = io.sdk(
            self.filesystem
                .workspace_for_creation_operation(
                    volume.storage_name(&mut None)?,
                    workspace.volume_config(),
                    operation,
                    &io,
                    io.cancellation(),
                )
                .await,
        )?;
        created
            .map(|generation| self.generation_ref_id(generation.id(), Some(&io)))
            .transpose()
    }

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
    ) -> Result<WorkspaceObservation>
    where
        A: 'static,
    {
        let host = self;

        if io.is_some_and(|io| io.original_native_work().is_some()) {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "volume fork does not accept task-bound native work",
            ));
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
                    let forked = io.sdk(
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
                    )?;
                    Ok::<_, Error>(forked)
                })
                .await,
        )??;
        io.remaining()?;
        host.fork_observation(&forked, name, operation, io).await
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
    /// Independent producers compose disjoint writes through the authority;
    /// observed data, policy and explicit generation conditions remain exact.
    pub(in crate::filesystem) fn volume_initialization(
        &self,
    ) -> acyclic_fs::VolumeInitialization<'static> {
        acyclic_fs::VolumeInitialization {
            config: acyclic_fs::VolumeConfig {
                concurrency: acyclic_fs::model::ConcurrencyMode::SerializedAuthority,
                ..acyclic_fs::VolumeConfig::portable(if self.filesystem.capabilities().durable {
                    acyclic_fs::Lifecycle::Durable
                } else {
                    acyclic_fs::Lifecycle::Ephemeral
                })
            },
            access_policy: Some(access::INITIAL_POLICY),
        }
    }

    fn initialization_for(&self, volume: &VolumeRef) -> acyclic_fs::VolumeInitialization<'static> {
        let mut initialization = self.volume_initialization();
        if volume.is_agent_journal() {
            initialization.access_policy = Some(include_bytes!("journal-policy.json"));
        }
        initialization
    }

    /// The one storage name and creation operation of fresh `volume` on this
    /// host, so every path creating it (alone or within another commit) names
    /// the identical generation and a retry replays exactly.
    pub(in crate::filesystem) fn volume_creation(
        &self,
        volume: &VolumeRef,
    ) -> Result<(String, acyclic_fs::OperationId)> {
        volume.validate(&mut None)?;
        if volume.provider() != &self.provider {
            return Err(Error::Invalid(
                "volume belongs to another Filesystem provider".into(),
            ));
        }
        let name = volume.storage_name(&mut None)?;
        let domain = if volume.is_agent_journal() {
            "acyclic harness protected journal initialization v2"
        } else {
            "acyclic harness volume initialization v1"
        };
        let digest = blake3::derive_key(domain, name.as_bytes());
        let mut operation = [0; 16];
        operation.copy_from_slice(&digest[..16]);
        Ok((name, acyclic_fs::OperationId::from_bytes(operation)))
    }

    /// Contributes the same initial generation as standalone volume creation.
    pub(in crate::filesystem) fn volume_create_spec(
        &self,
        volume: &VolumeRef,
    ) -> Result<acyclic_fs::CreateSpec<'static>> {
        let (name, operation) = self.volume_creation(volume)?;
        Ok(acyclic_fs::CreateSpec {
            name: acyclic_fs::WorkspaceName::new(
                name,
                &acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .map_err(|failure| map_error(failure.error))?
            .value,
            initialization: self.initialization_for(volume),
            creation: acyclic_fs::CreationOperation::populated(operation, &[]),
        })
    }

    /// Creates the unique physical workspace for a logical volume.
    pub async fn create_volume(&self, volume: &VolumeRef) -> Result<WorkspaceObservation> {
        self.create_volume_admitted(volume, None).await
    }

    pub(in crate::filesystem) async fn create_volume_admitted(
        &self,
        volume: &VolumeRef,
        scope: Option<&Scope>,
    ) -> Result<WorkspaceObservation> {
        let (name, operation) = self.volume_creation(volume)?;
        let cancellation = acyclic_fs::CancellationToken::new();
        let original = self
            .filesystem
            .workspace_for_creation_operation(
                &name,
                None,
                operation,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(|failure| map_error(failure.error))?;
        if original.value.is_none() {
            if let Some(scope) = scope {
                self.authorize_volume_creation(scope, volume)?;
            }
            self.filesystem
                .create_workspace_with_operation::<_, (), WorkspaceError>(
                    &name,
                    self.initialization_for(volume),
                    operation,
                    acyclic_fs::WorkBudget::UNBOUNDED,
                    &cancellation,
                    None,
                )
                .await
                .map_err(|failure| map_error(failure.error))?;
        }
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
