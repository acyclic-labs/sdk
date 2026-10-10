//! Task-bound access over the existing protected allocation and seed journals.

use super::*;
use crate::{
    Capabilities,
    core::Limits,
    conversation::{Attachment, ContentPublisher, FileRef, PrivateDirectoryPage},
    core::AuthorityIssuer,
    resource_preparation::{CreatedContentBinding, CreatedResourceOperation, CreatedResourceReceipt,
        ResourceCreationProofRef, ResourceCreationProvider, ResourcePreparationRequest},
    resources::GenerationRef,
    runtime::ContentBindings,
};
use super::super::{FilesystemContentPublisher, FilesystemContentVerifier};

/// Trusted creation verifier configured with host-managed signing authority.
/// Raw proof references never mint scopes; protected task-bound records must
/// match the entire admitted request before exact content handles are returned.
pub struct FilesystemResourceCreationProvider<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    issuer: AuthorityIssuer,
}

impl<A, O> FilesystemResourceCreationProvider<A, O> {
    /// Binds the Filesystem host and the preparation coordinator's issuer.
    pub fn new(host: Arc<FilesystemHost<A, O>>, issuer: AuthorityIssuer) -> Result<Self> {
        host.provider.validate()?;
        Ok(Self { host, issuer })
    }
}

struct AuthenticatedAllocation {
    origin: ResourceCreationOrigin,
    volume: VolumeRef,
    generation: GenerationRef,
    prepared_generation: GenerationRef,
    agent: crate::AgentId,
    authority: Option<Authority>,
    default_writer: bool,
    artifact_writer: bool,
    creation_operation: acyclic_fs::OperationId,
}

fn task_origin(claim: &AllocationClaim) -> Result<&ResourceCreationOrigin> {
    claim.task_origin.as_ref().ok_or_else(|| Error::Unauthorized(
        "raw trusted fork allocations cannot grant created-task access".into(),
    ))
}

/// Every invocation aggregates actual measured Filesystem IO under the original
/// request's finite bounds. No mutable volume head substitutes for evidence.
async fn authenticate<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>,
    owner: &Authority,
    request: &ResourcePreparationRequest,
    proof: &ResourceCreationProofRef,
) -> Result<AuthenticatedAllocation> {
    request.validate()?;
    proof.validate()?;
    if request.provider != host.provider || proof.provider != host.provider
        || request.creation_grant != capability::FORK_PUBLISH
        || !request.child.resource_proofs.contains(proof)
    {
        return Err(Error::Unauthorized("created access differs from the admitted provider boundary".into()));
    }
    let io = PreparationIo::new(request)?;
    io.encode(request, request.maximum_bytes)?;
    let record_limit = request.maximum_bytes.min(MAX_REPORT_BYTES);
    let fork = ForkRequest::deserialize(&request.payload)
        .map_err(|error| Error::Invalid(format!("fork preparation payload is invalid: {error}")))?;
    fork.validate()?;
    if serde_json::to_value(&fork).map_err(|error| Error::Invalid(error.to_string()))? != request.payload {
        return Err(Error::Unauthorized("fork payload is not the exact admitted request".into()));
    }
    let resources = creation_volumes(&fork).map(|(volume,_)| {
        if volume.provider() != &host.provider {
            return Err(Error::Unauthorized("created volume belongs to another provider".into()));
        }
        Ok(workspace_ref(host.provider.clone(), &volume.storage_name()?)?.as_resource().clone())
    }).collect::<Result<Vec<_>>>()?;
    if resources != request.resources {
        return Err(Error::Unauthorized("created identities differ from the exact fork allocation".into()));
    }
    let index = resources.iter().position(|resource| resource == &proof.resource)
        .ok_or_else(|| Error::Unauthorized("proof does not name a created fork volume".into()))?;
    let (selected_volume, _) = creation_volumes(&fork).nth(index)
        .ok_or_else(|| Error::Unauthorized("created allocation index is absent".into()))?;
    let evidence = allocation_ref(host.provider.clone(), selected_volume)?;
    if evidence.as_resource() != &proof.evidence {
        return Err(Error::Unauthorized("proof does not name this protected allocation journal".into()));
    }
    let journal = workspace_ref(host.provider.clone(), &format!("harness-fork-preparation-{}", fork.operation_id))?;
    if !read_request_claim(host, &journal, &fork, record_limit, Some(&io)).await? {
        return Err(Error::Unauthorized("fork preparation request is not retained".into()));
    }
    let report = read_preparation_report(host, &journal, &fork, record_limit, Some(&io)).await?
        .ok_or_else(|| Error::Unauthorized("fork allocation has no completed report".into()))?;
    let seed = report.into_seed()?;
    let digest = *blake3::hash(&io.encode(&fork, record_limit)?).as_bytes();
    let request_digest = crate::contract::canonical_json_digest(request)?;
    let binding = seed_binding_bounded(&seed, Some(&io))?;
    let mut selected = None;
    let mut original_origin = None;
    for (volume_index, (volume, role)) in creation_volumes(&fork).enumerate() {
        let allocation = allocation_ref(host.provider.clone(), volume)?;
        let claim = read_record_bounded::<A, O, AllocationClaim>(host, &allocation, "/claim.json", record_limit, Some(&io))
            .await?.ok_or_else(|| Error::Unauthorized("created allocation claim is missing".into()))?;
        let retained_binding = read_record_bounded::<A, O, SeedBinding>(host, &allocation, "/seed.json", record_limit, Some(&io))
            .await?.ok_or_else(|| Error::Unauthorized("created allocation seed is missing".into()))?;
        let origin = task_origin(&claim)?;
        if claim.operation_id != fork.operation_id || claim.preparation_digest != digest
            || claim.parent != fork.parent || claim.child != fork.child || claim.volume != *volume
            || claim.role != role
            || retained_binding != binding || &origin.owner != owner
            || origin.preparation_task != proof.preparation
            || Some(origin.parent_task) != request.child.parent
            || origin.intended_child != request.child.operation_id
            || origin.request_digest != request_digest
            || original_origin.as_ref().is_some_and(|prior| prior != origin)
        {
            return Err(Error::Unauthorized("created allocation does not authenticate the original task request".into()));
        }
        original_origin = Some(origin.clone());
        let prepared_generation = match role {
            ForkAllocationRole::Scratch => seed.child_private_generation.clone(),
            ForkAllocationRole::Project => seed.resources.iter().find_map(|resource| match &resource.revision {
                ResourceRevision::Project { volume: project, generation } if project == volume => Some(generation.clone()),
                _ => None,
            }).ok_or_else(|| Error::Unauthorized("original child project generation is absent".into()))?,
            ForkAllocationRole::Result => seed.child_result_generation.clone()
                .ok_or_else(|| Error::Unauthorized("original child result generation is absent".into()))?,
        };
        let workspace = workspace_ref(host.provider.clone(), &volume.storage_name()?)?;
        let original_workspace = bounded_io::open(host, &workspace, &io).await?;
        bounded_io::generation(host, &original_workspace, &prepared_generation, &io).await?;
        let creation_key = IdempotencyKey::new(format!(
            "fork:{}:{}:{}", fork.operation_id, hex::encode(digest),
            match role { ForkAllocationRole::Scratch => "scratch", ForkAllocationRole::Project => "project", ForkAllocationRole::Result => "result" },
        ))?;
        let initial = io.receive(original_workspace.operation_generation_measured(
            super::super::filesystem_key(&creation_key), io.remaining()?, &io.cancellation,
        ).await)?.ok_or_else(|| Error::Unauthorized("original fork allocation operation is missing".into()))?;
        let generation = host.generation_ref(&initial)?;
        let captured_generation = match role {
            ForkAllocationRole::Result => seed.child_result_generation.as_ref(),
            _ => seed.resources.iter().find_map(|resource| match &resource.revision {
                ResourceRevision::PrivateVolume { volume: captured, generation, .. }
                | ResourceRevision::Project { volume: captured, generation } if captured == volume => Some(generation),
                _ => None,
            }),
        }.ok_or_else(|| Error::Unauthorized("original allocated volume is absent from the seed".into()))?;
        if captured_generation != &generation {
            return Err(Error::Unauthorized("seed capture differs from the original allocation operation".into()));
        }
        if volume_index == index {
            selected = Some(AuthenticatedAllocation {
                origin: origin.clone(), volume: volume.clone(), generation, prepared_generation,
                agent: fork.child_agent, default_writer: role == ForkAllocationRole::Scratch,
                artifact_writer: role == ForkAllocationRole::Result,
                authority: (role == ForkAllocationRole::Scratch).then(|| fork.child.clone()),
                creation_operation: acyclic_fs::OperationId::from_bytes(super::super::filesystem_key(&creation_key).into_bytes()),
            });
        }
    }
    selected.ok_or_else(|| Error::Unauthorized("created allocation identity is absent".into()))
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ResourceCreationProvider
    for FilesystemResourceCreationProvider<A, O>
{
    fn provider(&self) -> &ProviderRef { &self.host.provider }

    fn verify<'a>(&'a self, request: &'a ResourcePreparationRequest, proof: &'a ResourceCreationProofRef)
        -> BoxFuture<'a, Result<CreatedResourceReceipt>>
    {
        Box::pin(async move {
            let allocation = authenticate(&self.host, self.issuer.verifier().audience(), request, proof).await?;
            let scope = self.issuer.root_for_agent(allocation.agent,
                format!("created:{}:{}", proof.preparation, allocation.volume.storage_name()?),
                Capabilities::new([
                    allocation.volume.capability(VolumeOperation::Read)?,
                    allocation.volume.capability(VolumeOperation::Write)?,
                ]));
            let access = Arc::new(CreatedFilesystemAccess {
                host: self.host.clone(), owner: self.issuer.verifier().audience().clone(),
                request: request.clone(), proof: proof.clone(),
                origin: allocation.origin.clone(), generation: allocation.generation.clone(),
                prepared_generation: allocation.prepared_generation,
                volume: allocation.volume.clone(),
                agent: allocation.agent, creation_operation: allocation.creation_operation,
                reader: FilesystemContentVerifier::new(self.host.clone(), self.issuer.verifier(), scope.clone(), request.maximum_bytes)?,
                writer: FilesystemContentPublisher::new(self.host.clone(), allocation.volume.clone(), &self.issuer.verifier(), &scope, request.maximum_bytes)?,
            });
            Ok(CreatedResourceReceipt {
                origin: allocation.origin, resource: proof.resource.clone(), evidence: proof.evidence.clone(),
                allocation_generation: allocation.generation,
                operations: vec![CreatedResourceOperation::ReadVolume(allocation.volume.clone()),
                    CreatedResourceOperation::WriteVolume(allocation.volume.clone())],
                content: vec![CreatedContentBinding {
                    volume: allocation.volume, default_writer: allocation.default_writer,
                    authority: allocation.authority,
                    artifact_writer: allocation.artifact_writer,
                    scratch_creation: allocation.default_writer.then(|| crate::resource_preparation::ChildScratchCreationProof::new(access.clone())),
                    binding: ContentBindings { reader: access.clone(), writer: Some(access) },
                }],
            })
        })
    }
}

struct CreatedFilesystemAccess<A, O> {
    host: Arc<FilesystemHost<A, O>>,
    owner: Authority,
    request: ResourcePreparationRequest,
    proof: ResourceCreationProofRef,
    origin: ResourceCreationOrigin,
    generation: GenerationRef,
    prepared_generation: GenerationRef,
    volume: VolumeRef,
    agent: crate::AgentId,
    creation_operation: acyclic_fs::OperationId,
    reader: FilesystemContentVerifier<A, O>,
    writer: FilesystemContentPublisher<A, O>,
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    crate::resource_preparation::ScratchCreationIdentity for CreatedFilesystemAccess<A,O>
{
    fn original_task(&self) -> crate::TaskId { crate::TaskId::from_bytes(self.origin.intended_child.into_bytes()) }
    fn agent(&self) -> crate::AgentId { self.agent }
    fn volume(&self) -> &VolumeRef { &self.volume }
    fn creation_operation(&self) -> acyclic_fs::OperationId { self.creation_operation }
    fn initial_generation(&self) -> &GenerationRef { &self.generation }
    fn origin(&self) -> &ResourceCreationOrigin { &self.origin }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CreatedFilesystemAccess<A, O> {
    async fn reauthenticate(&self, volume: &VolumeRef) -> Result<()> {
        if volume != &self.volume {
            return Err(Error::Unauthorized("created content handle names another volume".into()));
        }
        let current = authenticate(&self.host, &self.owner, &self.request, &self.proof).await?;
        if current.origin != self.origin || current.generation != self.generation
            || current.prepared_generation != self.prepared_generation || current.volume != self.volume
        {
            return Err(Error::Unauthorized("original created allocation evidence changed".into()));
        }
        Ok(())
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentResidencyVerifier for CreatedFilesystemAccess<A, O> {
    fn verify<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.reauthenticate(file.volume()).await?; self.reader.verify(file).await })
    }
    fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.reauthenticate(file.volume()).await?; self.reader.read(file).await })
    }
    fn read_private_prefix<'a>(&'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str, generation: &'a GenerationRef, maximum_bytes: u64) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.reauthenticate(volume).await?; self.reader.read_private_prefix(volume, prefix, path, generation, maximum_bytes).await })
    }
    fn list_private_directory<'a>(&'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str, generation: Option<&'a GenerationRef>, after: Option<&'a str>, maximum_entries: u32) -> BoxFuture<'a, Result<PrivateDirectoryPage>> {
        Box::pin(async move { self.reauthenticate(volume).await?; ContentResidencyVerifier::list_private_directory(&self.reader, volume, prefix, path, generation, after, maximum_entries).await })
    }
    fn read_private_path<'a>(&'a self, volume: &'a VolumeRef, prefix: &'a str, path: &'a str, generation: Option<&'a GenerationRef>) -> BoxFuture<'a, Result<(FileRef, Vec<u8>)>> {
        Box::pin(async move { self.reauthenticate(volume).await?; ContentResidencyVerifier::read_private_path(&self.reader, volume, prefix, path, generation).await })
    }
    fn verify_manifest<'a>(&'a self, file: &'a FileRef, count: u32, limits: &'a Limits) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.reauthenticate(file.volume()).await?; self.reader.verify_manifest(file, count, limits).await })
    }
    fn load_manifest<'a>(&'a self, file: &'a FileRef, count: u32) -> BoxFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async move { self.reauthenticate(file.volume()).await?; ContentResidencyVerifier::load_manifest(&self.reader, file, count).await })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentPublisher for CreatedFilesystemAccess<A, O> {
    fn volume(&self) -> &VolumeRef { &self.volume }
    fn stage<'a>(&'a self, operation: OperationId, path: &'a str, bytes: &'a [u8], media_type: &'a str, display_name: &'a str) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move { self.reauthenticate(&self.volume).await?; self.writer.stage(operation, path, bytes, media_type, display_name).await })
    }
    fn stage_once<'a>(&'a self, operation: OperationId, path: &'a str, bytes: &'a [u8], media_type: &'a str, display_name: &'a str) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move { self.reauthenticate(&self.volume).await?; self.writer.stage_once(operation, path, bytes, media_type, display_name).await })
    }
    fn stage_at<'a>(&'a self, operation: OperationId, source: &'a FileRef, bytes: &'a [u8]) -> BoxFuture<'a, Result<FileRef>> {
        Box::pin(async move { self.reauthenticate(source.volume()).await?; self.writer.stage_at(operation, source, bytes).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{VolumeClass, VolumeOwner};
    use crate::core::AggregateKind;

    #[test]
    fn raw_allocation_has_no_created_task_authority() -> Result<()> {
        let claim = AllocationClaim {
            operation_id: OperationId::from_bytes([81; 16]),
            preparation_digest: [82; 32],
            task_origin: None,
            parent: Authority { kind: AggregateKind::Task, id: "parent".into() },
            child: Authority { kind: AggregateKind::Task, id: "child".into() },
            volume: VolumeRef::new(
                ProviderRef::new("created-test", "filesystem", "2")?,
                "scratch", VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([83; 16])),
            )?,
            role: ForkAllocationRole::Scratch,
        };
        assert!(matches!(task_origin(&claim), Err(Error::Unauthorized(_))));
        Ok(())
    }
}
