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
    ) -> Result<WorkspaceObservation> {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        self.fork_volume_pinned(
            source,
            source_generation,
            destination,
            paths,
            idempotency_key,
        )
        .await
    }

    pub(in crate::filesystem) async fn fork_volume_pinned(
        &self,
        source: &VolumeRef,
        source_generation: &GenerationRef,
        child: &VolumeRef,
        paths: Option<Vec<String>>,
        idempotency_key: &IdempotencyKey,
    ) -> Result<WorkspaceObservation> {
        source.validate(&mut None)?;
        child.validate(&mut None)?;
        self.validate_provider(source.provider())?;
        self.validate_provider(child.provider())?;
        if source == child {
            return Err(Error::Invalid(
                "volume fork requires a distinct destination".into(),
            ));
        }
        let source_name = source.storage_name(&mut None)?;
        let source_workspace = self
            .open(&workspace_ref(self.provider.clone(), &source_name)?)
            .await?;
        let generation = self
            .generation(&source_workspace, source_generation)
            .await?;
        self.retain_generation(
            &workspace_ref(self.provider.clone(), &source_name)?,
            source_generation,
        )
        .await?;
        let child_name = child.storage_name(&mut None)?;
        let options = ForkOptions::from_generation(generation, filesystem_key(idempotency_key));
        let options = match paths {
            Some(paths) => options.inherit_paths(paths),
            None => options,
        };
        let forked = source_workspace
            .fork(&child_name, options, acyclic_fs::PublicationPermit::Unrestricted, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new(), |_| std::future::ready(Ok(acyclic_fs::OperationReceipt { value: (), work: acyclic_fs::WorkCounters::default() })))
            .await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?;
        let child_workspace = workspace_ref(self.provider.clone(), &child_name)?;
        let initial = forked
            .operation_generation(filesystem_key(idempotency_key), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?
            .ok_or_else(|| Error::Storage("fork creation has no durable generation".into()))?;
        let child_generation = self.generation_ref_id(initial.id())?;
        self.retain_generation(&child_workspace, &child_generation)
            .await?;
        Ok(WorkspaceObservation {
            workspace: child_workspace,
            generation: child_generation,
        })
    }

    /// Plans a destination-authorized volume import.
    pub async fn prepare_volume_import(
        &self,
        source: &VolumeRef,
        source_grant: &ContentGrant,
        generation: &GenerationRef,
        destination: &VolumeRef,
        destination_grant: &ContentGrant,
    ) -> Result<VolumeImportPlan<A, O>> {
        source_grant.require(source, VolumeOperation::Read, None)?;
        destination_grant.require(destination, VolumeOperation::Write, None)?;
        Ok(VolumeImportPlan {
            plan: self
                .plan_volume_import(source, generation, destination)
                .await?,
        })
    }

    pub(in crate::filesystem) async fn plan_volume_import(
        &self,
        source: &VolumeRef,
        generation: &GenerationRef,
        destination: &VolumeRef,
    ) -> Result<JoinPlan<A, O>> {
        source.validate(&mut None)?;
        destination.validate(&mut None)?;
        self.validate_provider(source.provider())?;
        self.validate_provider(destination.provider())?;
        let source_workspace = self
            .open(&workspace_ref(
                self.provider.clone(),
                &source.storage_name(&mut None)?,
            )?)
            .await?;
        let destination_workspace = self
            .open(&workspace_ref(
                self.provider.clone(),
                &destination.storage_name(&mut None)?,
            )?)
            .await?;
        let source_generation = self.generation(&source_workspace, generation).await?;
        let target = destination_workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?;
        source_workspace
            .join_into(&destination_workspace)
            .plan_at(&source_generation, &target, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))
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
        self.filesystem
            .create_workspace(&name, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
            .await.map(|receipt| receipt.value).map_err(|failure| map_error(failure.error))?;
        self.resolve(&workspace_ref(self.provider.clone(), &name)?)
            .await
    }
}
