use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Claims exact child-volume identities for a custom parent-authorized
    pub async fn claim_fork_seed(
        &self,
        seed: &ForkSeed,
        parent: &Reducer,
        verifier: &AuthorityVerifier,
        scope: &Scope,
    ) -> Result<()> {
        if seed.summary.is_some() {
            return Err(Error::Unsupported(
                "summary fork allocation requires its owner-bound checkpoint preparer".into(),
            ));
        }
        seed.validate(&mut None)?;
        if parent.authority() != &seed.parent || parent.revision() != seed.parent_revision {
            return Err(Error::Conflict(
                "fork allocation is not at the selected parent revision".into(),
            ));
        }
        let source_project = seed
            .resources
            .iter()
            .find_map(|resource| {
                if let ResourceRevision::Project { volume, .. } = &resource.source {
                    Some(volume)
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Invalid("fork has no source project".into()))?;
        let controller =
            ParentProjectController::new(self, parent, verifier, scope, source_project, None)?;
        controller.require(capability::FORK_PUBLISH, VolumeOperation::Read)?;
        let parent_agent = scope
            .agent()
            .ok_or_else(|| Error::Unauthorized("fork allocation requires a parent agent".into()))?;
        if parent_agent == seed.child_agent {
            return Err(Error::Unauthorized(
                "fork child must have a distinct agent identity".into(),
            ));
        }
        let conversation = parent
            .conversation()
            .ok_or_else(|| Error::Invalid("fork parent has no conversation state".into()))?;
        if let Some(file) = seed.inherited_context.first() {
            let bytes = self
                .read_pinned_io(file, file.descriptor().byte_length(), None)
                .await?;
            let actual: InheritedConversationPrefix = crate::contract::json_from_slice(&bytes)
                .map_err(|_| Error::Invalid("inherited conversation is malformed".into()))?;
            let expected = InheritedConversationPrefix::select(
                seed.parent.clone(),
                seed.parent_revision,
                seed.inherited_through_sequence,
                &seed.attached_agents,
                conversation,
                &mut None,
            )?;
            if actual != expected
                || crate::contract::canonical_json_bytes(&actual)?.as_slice() != bytes.as_ref()
            {
                return Err(Error::Conflict(
                    "inherited conversation differs from the selected parent prefix".into(),
                ));
            }
        }
        let digest = seed_binding(seed)?.digest;
        let child_project = seed
            .resources
            .iter()
            .find_map(|resource| {
                if let ResourceRevision::Project { volume, .. } = &resource.revision {
                    Some(volume)
                } else {
                    None
                }
            })
            .ok_or_else(|| Error::Invalid("fork has no child project".into()))?;
        if source_project.provider() != &self.provider
            || child_project.provider() != &self.provider
            || seed.child_private_volume.provider() != &self.provider
        {
            return Err(Error::Invalid(
                "fork allocation belongs to another Filesystem provider".into(),
            ));
        }
        for volume in [&seed.child_private_volume, child_project] {
            self.claim_fork_allocation(AllocationClaim {
                operation_id: seed.operation_id,
                preparation_digest: digest,
                parent: seed.parent.clone(),
                child: seed.child.clone(),
                volume: volume.clone(),
            })
            .await?;
        }
        self.bind_fork_seed(seed).await
    }

    pub(crate) async fn verify_fork_allocation(
        &self,
        seed: &ForkSeed,
        volume: &VolumeRef,
    ) -> Result<()> {
        self.allocation_claim(seed, volume).await?;
        let journal = allocation_ref(&self.provider, volume, None)?;
        let binding =
            read_record::<A, O, SeedBinding>(self, &journal, "/seed.json", MAX_REPORT_BYTES)
                .await?
                .ok_or_else(|| {
                    Error::Unauthorized("fork seed was not bound to its allocation".into())
                })?;
        exact(binding, &seed_binding(seed)?)?;
        Ok(())
    }
    async fn allocation_claim(&self, seed: &ForkSeed, volume: &VolumeRef) -> Result<WorkspaceRef> {
        let journal = allocation_ref(&self.provider, volume, None)?;
        let claim =
            read_record::<A, O, AllocationClaim>(self, &journal, "/claim.json", MAX_REQUEST_BYTES)
                .await?
                .ok_or_else(|| Error::Unauthorized("fork child volume was not allocated".into()))?;
        exact(
            (
                claim.operation_id,
                &claim.parent,
                &claim.child,
                &claim.volume,
            ),
            &(seed.operation_id, &seed.parent, &seed.child, volume),
        )?;
        Ok(journal)
    }
    pub(super) async fn bind_fork_seed(&self, seed: &ForkSeed) -> Result<()> {
        let binding = seed_binding(seed)?;
        let project = seed
            .resources
            .iter()
            .find_map(|resource| match &resource.revision {
                ResourceRevision::Project { volume, .. } => Some(volume),
                _ => None,
            })
            .ok_or_else(|| Error::Invalid("fork has no child project".into()))?;
        let key = IdempotencyKey::new(format!("fork:{}:seed", seed.operation_id))?;
        for volume in [&seed.child_private_volume, project] {
            let journal = self.allocation_claim(seed, volume).await?;
            exact(
                persist(
                    self,
                    &journal,
                    "/seed.json",
                    MAX_REPORT_BYTES,
                    &binding,
                    &key,
                    seed.operation_id,
                )
                .await?,
                &binding,
            )?;
        }
        Ok(())
    }
    pub(super) async fn claim_fork_allocation(&self, claim: AllocationClaim) -> Result<()> {
        claim.volume.validate(&mut None)?;
        if claim.volume.provider() != &self.provider {
            return Err(Error::Invalid("foreign fork allocation provider".into()));
        }
        let journal = allocation_ref(&self.provider, &claim.volume, None)?;
        let name = std::str::from_utf8(journal.as_resource().key())
            .map_err(|_| Error::Invalid("allocation journal key is not UTF-8".into()))?;
        self.filesystem
            .create_workspace(
                name,
                acyclic_fs::WorkBudget::UNBOUNDED,
                &acyclic_fs::CancellationToken::new(),
            )
            .await
            .map_err(map_error)?;
        let key = IdempotencyKey::new(format!("fork:{}:allocation", claim.operation_id))?;
        exact(
            persist(
                self,
                &journal,
                "/claim.json",
                MAX_REQUEST_BYTES,
                &claim,
                &key,
                claim.operation_id,
            )
            .await?,
            &claim,
        )?;
        Ok(())
    }
}

pub(super) fn allocation_ref(
    provider: &ProviderRef,
    volume: &VolumeRef,
    io: Option<&PreparationIo>,
) -> Result<WorkspaceRef> {
    let volume_name = (|| -> crate::Result<_> {
        let volume = volume;
        let io: Option<&crate::filesystem::PreparationIo> = io;

        match io {
            Some(io) => io.capability(volume, "harness-"),
            None => volume.storage_name(&mut None),
        }
    })()?;
    let held = io
        .map(|io| io.guard_owned(volume_name.capacity() as u64))
        .transpose()?;
    let name = crate::executor::with_history_io(io, |work| {
        crate::contract::preparation_format(format_args!("fork-allocation-{volume_name}"), work)
    })?;
    drop(volume_name);
    drop(held);
    let provider = match io {
        Some(io) => provider.clone_with_io(io)?,
        None => provider.clone(),
    };
    WorkspaceRef::new(provider, name.into_bytes(), None)
}
