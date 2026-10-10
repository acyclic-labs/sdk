//! Task-bound access over the existing protected allocation and seed journals.

use super::*;
use crate::{
    Capabilities,
    conversation::ContentFuture,
    core::Limits,
    conversation::{Attachment, ContentPublisher, FileRef, PrivateDirectoryPage},
    core::AuthorityIssuer,
    resource_preparation::{CreatedContentBinding, CreatedReferenceBinding, CreatedResourceOperation,
        CreatedResourceReceipt, ResourceCreationProofRef, ResourceCreationProvider, ResourcePreparationRequest},
    resources::GenerationRef,
    runtime::ContentBindings,
};
use super::super::{FilesystemContentPublisher, FilesystemContentVerifier};
use acyclic_fs::{CancellationToken, MeasuredResult, OperationFailure, OperationReceipt, WorkBudget, WorkCounters};

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
    reference_files: Vec<FileRef>,
}

fn task_origin(claim: &AllocationClaim) -> Result<&ResourceCreationOrigin> {
    claim.task_origin.as_ref().ok_or_else(|| Error::Unauthorized(
        "raw trusted fork allocations cannot grant created-task access".into(),
    ))
}

/// Every invocation aggregates actual measured Filesystem IO under the original
/// request's finite bounds. No mutable volume head substitutes for evidence.
async fn authenticate<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, owner: &Authority,
    request: &ResourcePreparationRequest, proof: &ResourceCreationProofRef,
) -> Result<AuthenticatedAllocation> {
    let io = PreparationIo::new(request)?;
    authenticate_with_io(host, owner, request, proof, &io).await
}

async fn authenticate_with_io<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, owner: &Authority,
    request: &ResourcePreparationRequest, proof: &ResourceCreationProofRef,
    io: &PreparationIo,
) -> Result<AuthenticatedAllocation> {
    request.validate()?;
    proof.validate()?;
    if request.provider != host.provider || proof.provider != host.provider
        || request.creation_grant != capability::FORK_PUBLISH
        || !request.child.resource_proofs.contains(proof)
    {
        return Err(Error::Unauthorized("created access differs from the admitted provider boundary".into()));
    }
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
    if !read_request_claim(host, &journal, &fork, record_limit, Some(io)).await? {
        return Err(Error::Unauthorized("fork preparation request is not retained".into()));
    }
    let report = read_preparation_report(host, &journal, &fork, record_limit, Some(io)).await?
        .ok_or_else(|| Error::Unauthorized("fork allocation has no completed report".into()))?;
    let seed = report.into_seed()?;
    let digest = *blake3::hash(&io.encode(&fork, record_limit)?).as_bytes();
    let request_digest = crate::contract::canonical_json_digest(request)?;
    let binding = seed_binding_bounded(&seed, Some(io))?;
    let mut selected: Option<AuthenticatedAllocation> = None;
    let mut original_origin: Option<ResourceCreationOrigin> = None;
    for (volume_index, (volume, role)) in creation_volumes(&fork).enumerate() {
        let allocation = allocation_ref(host.provider.clone(), volume)?;
        let mut claim = read_record_bounded::<A, O, AllocationClaim>(host, &allocation, "/claim.json", record_limit, Some(io))
            .await?.ok_or_else(|| Error::Unauthorized("created allocation claim is missing".into()))?;
        let retained_binding = read_record_bounded::<A, O, SeedBinding>(host, &allocation, "/seed.json", record_limit, Some(io))
            .await?.ok_or_else(|| Error::Unauthorized("created allocation seed is missing".into()))?;
        let origin = task_origin(&claim)?;
        let prior_origin = selected.as_ref().map(|known| &known.origin).or(original_origin.as_ref());
        if claim.operation_id != fork.operation_id || claim.preparation_digest != digest
            || claim.parent != fork.parent || claim.child != fork.child || claim.volume != *volume
            || claim.role != role
            || retained_binding != binding || &origin.owner != owner
            || origin.preparation_task != proof.preparation
            || Some(origin.parent_task) != request.child.parent
            || origin.intended_child != request.child.operation_id
            || origin.request_digest != request_digest
            || prior_origin.is_some_and(|prior| prior != origin)
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
        let original_workspace = bounded_io::open(host, &workspace, io).await?;
        bounded_io::generation(host, &original_workspace, &prepared_generation, io).await?;
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
                reference_files: Vec::new(),
            });
        }
        if original_origin.is_none() && selected.is_none() {
            original_origin = claim.task_origin.take();
        }
    }
    let mut selected = selected.ok_or_else(|| Error::Unauthorized("created allocation identity is absent".into()))?;
    let mut granted = BTreeSet::new();
    for reference in seed.reference_grants {
        require_reference_donor(&selected.origin.source_grants, &reference)?;
        if reference.attachment_manifest.as_ref().is_some_and(|manifest|
            !seed.attachment_manifests.contains(manifest))
        {
            return Err(Error::Unauthorized("inherited member proof is not retained in the original seed".into()));
        }
        if reference.reader == seed.child_agent {
            let capability = reference.capability()?;
            if !request.child.grants.contains(&capability) {
                return Err(Error::Unauthorized("child admission omitted its sealed exact-file read grant".into()));
            }
            if selected.default_writer && granted.insert(capability) {
                selected.reference_files.push(reference.file);
            }
        }
    }
    for file in &request.input_files {
        if !crate::runtime::read_granted(&selected.origin.source_grants, file)?
            || !request.child.grants.contains(&file.read_capability()?)
        {
            return Err(Error::Unauthorized("child input lacks original donor and exact child read authority".into()));
        }
        if selected.default_writer {
            // Same finite ledger, immutable generation, byte digest and retention
            // machinery as ordinary provider reads; no mutable head substitution.
            let bytes = host.read_pinned_io(file, request.maximum_bytes, Some(io)).await?;
            let length = bytes.len() as u64;
            drop(bytes);
            io.release(length)?;
            if granted.insert(file.read_capability()?) {
                selected.reference_files.push(file.clone());
            }
        }
    }
    Ok(selected)
}

/// Caller work and original creation limits constrain the same actual ledger.
fn content_io(request: &ResourcePreparationRequest, mut budget: WorkBudget,
    cancellation: &CancellationToken) -> Result<PreparationIo> {
    macro_rules! cap {
        ($limit:expr; $($field:ident),+ $(,)?) => { $(budget.$field = budget.$field.min($limit);)+ };
    }
    cap!(request.maximum_work; authority_records_read, authority_records_appended,
        object_probes, backend_read_operations, backend_write_operations, durability_operations,
        page_reads, page_writes, source_path_components, source_entries_visited,
        items_examined, items_returned, allocation_operations, materializations);
    cap!(request.maximum_bytes; authority_bytes_read, authority_bytes_written,
        object_bytes_read, object_bytes_written, bytes_hashed, bytes_copied, bytes_encoded,
        source_bytes_read, output_bytes, peak_allocation_bytes);
    PreparationIo::from_budget(budget, cancellation)
}

fn content_result<T>(io: &PreparationIo, result: Result<T>) -> MeasuredResult<OperationReceipt<T>, Error> {
    match result {
        Ok(value) => Ok(OperationReceipt { value, work: io.receipt() }),
        Err(error) => Err(OperationFailure::new(error, io.receipt())),
    }
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
            let mut operations = Vec::with_capacity(2 + allocation.reference_files.len());
            let mut references = Vec::with_capacity(allocation.reference_files.len());
            operations.push(CreatedResourceOperation::ReadVolume(allocation.volume.clone()));
            operations.push(CreatedResourceOperation::WriteVolume(allocation.volume.clone()));
            for file in allocation.reference_files {
                let source_agent = match file.volume().owner() {
                    crate::conversation::VolumeOwner::Agent(agent) => *agent,
                    _ => allocation.agent,
                };
                let reference_scope = self.issuer.root_for_agent(source_agent,
                    format!("created-reference:{}:{}", proof.preparation, file.read_capability()?),
                    Capabilities::new([file.read_capability()?]));
                let reader = Arc::new(CreatedFilesystemReference {
                    allocation: access.clone(), file: file.clone(),
                    reader: FilesystemContentVerifier::new(self.host.clone(), self.issuer.verifier(),
                        reference_scope, request.maximum_bytes)?,
                });
                operations.push(CreatedResourceOperation::ReadFile(file.clone()));
                references.push(CreatedReferenceBinding { file, reader });
            }
            Ok(CreatedResourceReceipt {
                origin: allocation.origin, resource: proof.resource.clone(), evidence: proof.evidence.clone(),
                allocation_generation: allocation.generation,
                operations,
                references,
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
        self.validate_authenticated(&current)
    }
    fn validate_authenticated(&self, current: &AuthenticatedAllocation) -> Result<()> {
        if current.origin != self.origin || current.generation != self.generation
            || current.prepared_generation != self.prepared_generation || current.volume != self.volume
        {
            return Err(Error::Unauthorized("original created allocation evidence changed".into()));
        }
        Ok(())
    }
    async fn reauthenticate_with_io(&self, volume: &VolumeRef, io: &PreparationIo) -> Result<()> {
        if volume != &self.volume {
            return Err(Error::Unauthorized("created content handle names another volume".into()));
        }
        let current = authenticate_with_io(&self.host, &self.owner, &self.request, &self.proof, io).await?;
        self.validate_authenticated(&current)
    }
}

/// Revalidates the original creator seal and admits only its one immutable file.
/// Its signer scope contains no volume, directory or publication capability.
struct CreatedFilesystemReference<A, O> {
    allocation: Arc<CreatedFilesystemAccess<A, O>>,
    file: FileRef,
    reader: FilesystemContentVerifier<A, O>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> CreatedFilesystemReference<A, O> {
    async fn reauthenticate(&self, file: &FileRef) -> Result<()> {
        if file != &self.file {
            return Err(Error::Unauthorized("created reference handle names another immutable file".into()));
        }
        let allocation = &self.allocation;
        let current = authenticate(&allocation.host, &allocation.owner, &allocation.request, &allocation.proof).await?;
        allocation.validate_authenticated(&current)?;
        if !current.reference_files.contains(file)
        {
            return Err(Error::Unauthorized("original inherited reference evidence changed".into()));
        }
        Ok(())
    }
    async fn reauthenticate_with_io(&self, file: &FileRef, io: &PreparationIo) -> Result<()> {
        if file != &self.file {
            return Err(Error::Unauthorized("created reference handle names another immutable file".into()));
        }
        let allocation = &self.allocation;
        let current = authenticate_with_io(&allocation.host, &allocation.owner,
            &allocation.request, &allocation.proof, io).await?;
        allocation.validate_authenticated(&current)?;
        if !current.reference_files.contains(file) {
            return Err(Error::Unauthorized("original inherited reference evidence changed".into()));
        }
        Ok(())
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentResidencyVerifier for CreatedFilesystemReference<A, O> {
    fn verify<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.reauthenticate(file).await?; self.reader.verify(file).await })
    }
    fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.reauthenticate(file).await?; self.reader.read(file).await })
    }
    fn supports_measured_read(&self, volume: &VolumeRef, budget: WorkBudget, cancellation: &CancellationToken)
        -> MeasuredResult<OperationReceipt<bool>, Error> {
        if volume != self.file.volume() {
            return Ok(OperationReceipt { value: false, work: WorkCounters::default() });
        }
        self.reader.supports_measured_read(volume, budget, cancellation)
    }
    fn read_measured<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<Vec<u8>>, Error>> {
        Box::pin(async move {
            let io = content_io(&self.allocation.request, budget, cancellation).map_err(OperationFailure::before_work)?;
            let result = async {
                self.reauthenticate_with_io(file, &io).await?;
                io.receive_content(self.reader.read_measured(file, maximum_bytes.min(self.allocation.request.maximum_bytes),
                    io.remaining()?, cancellation).await)
            }.await;
            content_result(&io, result)
        })
    }
    fn verify_measured<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<()>, Error>> {
        Box::pin(async move {
            self.read_measured(file, maximum_bytes, budget, cancellation).await
                .map(|receipt| OperationReceipt { value: (), work: receipt.work })
        })
    }
    fn verify_manifest<'a>(&'a self, file: &'a FileRef, count: u32, limits: &'a Limits) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.reauthenticate(file).await?; self.reader.verify_manifest(file, count, limits).await })
    }
    fn load_manifest<'a>(&'a self, file: &'a FileRef, count: u32) -> BoxFuture<'a, Result<Vec<Attachment>>> {
        Box::pin(async move { self.reauthenticate(file).await?; ContentResidencyVerifier::load_manifest(&self.reader, file, count).await })
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> ContentResidencyVerifier for CreatedFilesystemAccess<A, O> {
    fn verify<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move { self.reauthenticate(file.volume()).await?; self.reader.verify(file).await })
    }
    fn read<'a>(&'a self, file: &'a FileRef) -> BoxFuture<'a, Result<Vec<u8>>> {
        Box::pin(async move { self.reauthenticate(file.volume()).await?; self.reader.read(file).await })
    }
    fn supports_measured_read(&self, volume: &VolumeRef, budget: WorkBudget, cancellation: &CancellationToken)
        -> MeasuredResult<OperationReceipt<bool>, Error> {
        if volume != &self.volume {
            return Ok(OperationReceipt { value: false, work: WorkCounters::default() });
        }
        self.reader.supports_measured_read(volume, budget, cancellation)
    }
    fn read_measured<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<Vec<u8>>, Error>> {
        Box::pin(async move {
            let io = content_io(&self.request, budget, cancellation).map_err(OperationFailure::before_work)?;
            let result = async {
                self.reauthenticate_with_io(file.volume(), &io).await?;
                io.receive_content(self.reader.read_measured(file, maximum_bytes.min(self.request.maximum_bytes),
                    io.remaining()?, cancellation).await)
            }.await;
            content_result(&io, result)
        })
    }
    fn verify_measured<'a>(&'a self, file: &'a FileRef, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<()>, Error>> {
        Box::pin(async move {
            self.read_measured(file, maximum_bytes, budget, cancellation).await
                .map(|receipt| OperationReceipt { value: (), work: receipt.work })
        })
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
    fn observe_staged_once<'a>(&'a self, operation: OperationId, maximum_bytes: u64) -> BoxFuture<'a, Result<Option<FileRef>>> {
        Box::pin(async move {
            self.reauthenticate(&self.volume).await?;
            self.writer.observe_staged_once(operation, maximum_bytes.min(self.request.maximum_bytes)).await
        })
    }
    fn stage_once_measured<'a>(&'a self, operation: OperationId, path: &'a str, bytes: &'a [u8],
        media_type: &'a str, display_name: &'a str, budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<FileRef>, Error>> {
        Box::pin(async move {
            let io = content_io(&self.request, budget, cancellation).map_err(OperationFailure::before_work)?;
            let result = async {
                self.reauthenticate_with_io(&self.volume, &io).await?;
                io.receive_content(self.writer.stage_once_measured(operation, path, bytes, media_type, display_name,
                    io.remaining()?, cancellation).await)
            }.await;
            content_result(&io, result)
        })
    }
    fn observe_staged_once_measured<'a>(&'a self, operation: OperationId, maximum_bytes: u64,
        budget: WorkBudget, cancellation: &'a CancellationToken)
        -> ContentFuture<'a, MeasuredResult<OperationReceipt<Option<FileRef>>, Error>> {
        Box::pin(async move {
            let io = content_io(&self.request, budget, cancellation).map_err(OperationFailure::before_work)?;
            let result = async {
                self.reauthenticate_with_io(&self.volume, &io).await?;
                io.receive_content(self.writer.observe_staged_once_measured(operation, maximum_bytes.min(self.request.maximum_bytes),
                    io.remaining()?, cancellation).await)
            }.await;
            content_result(&io, result)
        })
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
