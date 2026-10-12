use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SandboxAllocation {
    origin: ResourceCreationOrigin,
    request: ResourcePreparationRequest,
    resource: ResourceRef,
    evidence: ResourceRef,
    workspace: WorkspaceRef,
    sandbox_generation: GenerationRef,
}

/// A genuine custom creator: an isolated workspace and a separate, immutable allocation record.
pub(super) struct SandboxCreator {
    pub(super) provider: ProviderRef,
    pub(super) filesystem_provider: ProviderRef,
    pub(super) filesystem: Arc<MemoryFilesystem>,
    pub(super) allocations: Mutex<BTreeMap<Vec<u8>, GenerationRef>>,
}

impl SandboxCreator {
    pub(super) fn proof(&self, preparation: TaskId) -> Result<ResourceCreationProofRef> {
        let evidence_volume = VolumeRef::new(
            self.filesystem_provider.clone(),
            "sandbox-allocation".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([81; 16])),
            &mut None,
        )?;
        Ok(ResourceCreationProofRef {
            provider: self.provider.clone(),
            preparation,
            resource: ResourceRef::new(
                ResourceKind::Sandbox,
                self.provider.clone(),
                b"isolated-sandbox".to_vec().into(),
                None,
                &mut None,
            )?,
            evidence: workspace_ref(
                self.filesystem_provider.clone(),
                &evidence_volume.storage_name(&mut None)?,
            )?
            .as_resource()
            .clone(),
        })
    }

    pub(super) async fn create(
        &self,
        preparation: &TaskResourcePreparation,
        corrupt_origin: impl FnOnce(&mut ResourceCreationOrigin),
    ) -> Result<()> {
        preparation.verify_creation().await?;
        let request = preparation.request();
        request.validate(&mut None)?;
        let proof = self.proof(preparation.origin().preparation_task)?;
        if request.provider != self.provider
            || request.creation_grant != "resource:create:custom"
            || request.resources != [proof.resource.clone()]
            || request.child.resource_proofs != [proof.clone()]
            || request.maximum_work < 4
        {
            return Err(Error::Unauthorized(
                "sandbox creation request changed".into(),
            ));
        }
        let sandbox = VolumeRef::new(
            self.filesystem_provider.clone(),
            "isolated-sandbox".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([81; 16])),
            &mut None,
        )?;
        let workspace = self.filesystem.create_volume(&sandbox).await?.workspace;
        let payload = crate::contract::canonical_json_bytes(&request.payload)?;
        let manifest_bytes = payload.len() as u64;
        let sandbox_generation = self
            .filesystem
            .apply(
                &workspace,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/manifest.json".into(),
                    bytes: payload.into(),
                }],
                &IdempotencyKey::new("sandbox-manifest")?,
            )
            .await?;
        preparation.verify_creation().await?;
        let evidence_volume = VolumeRef::new(
            self.filesystem_provider.clone(),
            "sandbox-allocation".into(),
            VolumeClass::AgentPrivate,
            VolumeOwner::Agent(AgentId::from_bytes([81; 16])),
            &mut None,
        )?;
        let evidence = self
            .filesystem
            .create_volume(&evidence_volume)
            .await?
            .workspace;
        let expected_evidence = workspace_ref(
            self.filesystem_provider.clone(),
            &evidence_volume.storage_name(&mut None)?,
        )?;
        let mut origin = preparation.origin().clone();
        corrupt_origin(&mut origin);
        let allocation = SandboxAllocation {
            origin,
            request: request.clone(),
            resource: proof.resource.clone(),
            evidence: proof.evidence.clone(),
            workspace,
            sandbox_generation,
        };
        assert_eq!(evidence, expected_evidence);
        let bytes = crate::contract::canonical_json_bytes(&allocation)?;
        crate::contract::validate_json_byte_bound(&allocation, request.maximum_bytes)?;
        if manifest_bytes.saturating_add(bytes.len() as u64) > request.maximum_bytes {
            return Err(Error::Invalid(
                "sandbox allocation exceeds its aggregate IO bound".into(),
            ));
        }
        let generation = self
            .filesystem
            .apply(
                &evidence,
                None,
                &[WorkspaceMutation::PutFile {
                    path: "/allocation.json".into(),
                    bytes: bytes.into(),
                }],
                &IdempotencyKey::new("sandbox-allocation")?,
            )
            .await?;
        preparation.verify_creation().await?;
        self.allocations.lock().await.insert(
            crate::contract::canonical_json_bytes(&proof.resource)?,
            generation,
        );
        Ok(())
    }
}

impl ResourceCreationProvider for SandboxCreator {
    fn provider(&self) -> &ProviderRef {
        &self.provider
    }

    fn verify<'a>(
        &'a self,
        request: &'a ResourcePreparationRequest,
        proof: &'a ResourceCreationProofRef,
    ) -> BoxProviderFuture<'a, Result<CreatedResourceReceipt>> {
        Box::pin(async move {
            request.validate(&mut None)?;
            if request.provider != self.provider || proof != &self.proof(proof.preparation)? {
                return Err(Error::Unauthorized("sandbox proof identity changed".into()));
            }
            let generation = self
                .allocations
                .lock()
                .await
                .get(&crate::contract::canonical_json_bytes(&proof.resource)?)
                .cloned()
                .ok_or_else(|| Error::Unauthorized("sandbox allocation is absent".into()))?;
            let volume = VolumeRef::new(
                self.filesystem_provider.clone(),
                "sandbox-allocation".into(),
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([81; 16])),
                &mut None,
            )?;
            let evidence = workspace_ref(
                self.filesystem_provider.clone(),
                &volume.storage_name(&mut None)?,
            )?;
            let bytes = self
                .filesystem
                .read(
                    &evidence,
                    Some(&generation),
                    "/allocation.json",
                    request.maximum_bytes,
                )
                .await?;
            let allocation: SandboxAllocation = crate::contract::json_from_slice(&bytes)
                .map_err(|error| Error::Storage(error.to_string()))?;
            if crate::contract::canonical_json_bytes(&allocation)? != bytes.as_ref()
                || &allocation.request != request
                || allocation.resource != proof.resource
                || allocation.evidence != proof.evidence
                || allocation.origin.preparation_task != proof.preparation
            {
                return Err(Error::Unauthorized(
                    "sandbox allocation differs from request".into(),
                ));
            }
            let manifest = self
                .filesystem
                .read(
                    &allocation.workspace,
                    Some(&allocation.sandbox_generation),
                    "/manifest.json",
                    request.maximum_bytes,
                )
                .await?;
            if manifest.as_ref() != crate::contract::canonical_json_bytes(&request.payload)? {
                return Err(Error::Unauthorized(
                    "sandbox allocation payload changed".into(),
                ));
            }
            Ok(CreatedResourceReceipt {
                origin: allocation.origin,
                resource: allocation.resource,
                evidence: allocation.evidence,
                allocation_generation: generation,
                operations: vec![CreatedResourceOperation::Named("execute".into())],
                content: Vec::new(),
                references: Vec::new(),
            })
        })
    }
}
