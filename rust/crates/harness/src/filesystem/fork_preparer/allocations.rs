//! Protected child allocation claims and seed publication.
use super::*;
use crate::conversation::FileRef;
pub(super) fn creation_volumes(request: &ForkRequest) -> impl Iterator<Item = &VolumeRef> {
    [
        Some(&request.preparation.child_private_volume),
        Some(&request.preparation.child_project_volume),
        request.preparation.child_result_volume.as_ref(),
    ]
    .into_iter()
    .flatten()
}

pub(super) fn seed_creation_volumes(seed: &ForkSeed) -> Result<impl Iterator<Item = &VolumeRef>> {
    let project = seed
        .resources
        .iter()
        .find_map(|resource| match &resource.revision {
            ResourceRevision::Project { volume, .. } => Some(volume),
            _ => None,
        })
        .ok_or_else(|| Error::Invalid("fork has no child project".into()))?;
    Ok([
        Some(&seed.child_private_volume),
        Some(project),
        seed.child_result_volume.as_ref(),
    ]
    .into_iter()
    .flatten())
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AllocationClaim {
    pub(super) operation_id: OperationId,
    pub(super) preparation_digest: [u8; 32],
    pub(crate) task_origin: Option<ResourceCreationOrigin>,
    pub(super) parent: Authority,
    pub(super) child: Authority,
    pub(super) volume: VolumeRef,
}

impl bounded_io::UploadRecord for [u8; 32] {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(0)
    }
}

impl bounded_io::UploadRecord for AllocationClaim {
    fn logical_owned_bytes(&self) -> Result<u64> {
        let mut bytes = crate::core::owned_sum([
            self.parent.id.capacity() as u64,
            self.child.id.capacity() as u64,
            self.volume.logical_owned_bytes()?,
        ])?;
        if let Some(origin) = &self.task_origin {
            bytes = crate::core::owned_sum([
                bytes,
                origin.owner.id.capacity() as u64,
                origin.creation_fence.reservation_id.capacity() as u64,
                origin.creation_fence.placement.capacity() as u64,
                origin.source_grants.logical_owned_bytes()?,
            ])?;
        }
        Ok(bytes)
    }
}

fn revision_owned_bytes(revision: &ResourceRevision) -> Result<u64> {
    use crate::core::{owned_sum, owned_vector};
    match revision {
        ResourceRevision::History(reference) => reference.as_resource().logical_owned_bytes(),
        ResourceRevision::Context(reference) => reference.as_resource().logical_owned_bytes(),
        ResourceRevision::Process(reference) => reference.logical_owned_bytes(),
        ResourceRevision::Artifact(reference) => reference.as_resource().logical_owned_bytes(),
        ResourceRevision::SharedVolume(volume) => volume.logical_owned_bytes(),
        ResourceRevision::Project { volume, generation } => owned_sum([
            volume.logical_owned_bytes()?,
            generation.logical_owned_bytes()?,
        ]),
        ResourceRevision::PrivateVolume {
            volume,
            generation,
            paths,
        } => owned_sum([
            volume.logical_owned_bytes()?,
            generation.logical_owned_bytes()?,
            owned_vector(paths, |path| Ok(path.capacity() as u64))?,
        ]),
        ResourceRevision::Extension {
            name, reference, ..
        } => owned_sum([name.capacity() as u64, reference.logical_owned_bytes()?]),
    }
}

impl bounded_io::UploadRecord for ForkRequest {
    fn logical_owned_bytes(&self) -> Result<u64> {
        use crate::core::{owned_sum, owned_vector};
        let revisions = owned_vector(&self.selections, |selection| {
            revision_owned_bytes(&selection.revision)
        })?;
        owned_sum([
            self.parent.id.capacity() as u64,
            self.child.id.capacity() as u64,
            crate::core::owned_buffer::<crate::AgentId>(self.attached_agents.capacity())?,
            self.preparation
                .child_project_volume
                .logical_owned_bytes()?,
            self.preparation
                .child_private_volume
                .logical_owned_bytes()?,
            self.preparation
                .child_result_volume
                .as_ref()
                .map(VolumeRef::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0),
            self.preparation
                .summary
                .as_ref()
                .map(|summary| summary.checkpoint.logical_owned_bytes())
                .transpose()?
                .unwrap_or(0),
            revisions,
            self.boundary
                .as_ref()
                .map(|boundary| {
                    owned_sum([
                        boundary.provider.logical_owned_bytes()?,
                        boundary.evidence.capacity() as u64,
                    ])
                })
                .transpose()?
                .unwrap_or(0),
            self.authorized_source
                .as_ref()
                .map(crate::source_authorization::ProjectSourceAuthorization::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0),
        ])
    }
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::ForkRequest
    }
}

impl bounded_io::UploadRecord for ForkReport {
    fn logical_owned_bytes(&self) -> Result<u64> {
        use crate::core::{owned_buffer, owned_sum, owned_vector};
        owned_sum([
            bounded_io::UploadRecord::logical_owned_bytes(&self.request)?,
            owned_vector(
                &self.captures,
                bounded_io::UploadRecord::logical_owned_bytes,
            )?,
            self.child_private_volume.logical_owned_bytes()?,
            self.child_private_generation.logical_owned_bytes()?,
            self.child_result_volume
                .as_ref()
                .map(VolumeRef::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0),
            self.child_result_generation
                .as_ref()
                .map(crate::resources::GenerationRef::logical_owned_bytes)
                .transpose()?
                .unwrap_or(0),
            owned_vector(&self.inherited_context, FileRef::logical_owned_bytes)?,
            self.summary
                .as_ref()
                .map(|summary| {
                    owned_sum([
                        summary.selection.checkpoint.logical_owned_bytes()?,
                        summary.context.logical_owned_bytes()?,
                        owned_vector(&summary.payloads, FileRef::logical_owned_bytes)?,
                        owned_vector(&summary.references, FileRef::logical_owned_bytes)?,
                    ])
                })
                .transpose()?
                .unwrap_or(0),
            owned_vector(&self.shared_grants, |grant| {
                owned_sum([
                    grant.volume.logical_owned_bytes()?,
                    owned_buffer::<VolumeOperation>(grant.operations.len())?,
                ])
            })?,
            owned_vector(&self.reference_grants, |grant| {
                owned_sum([
                    grant.file.logical_owned_bytes()?,
                    grant
                        .attachment_manifest
                        .as_ref()
                        .map(FileRef::logical_owned_bytes)
                        .transpose()?
                        .unwrap_or(0),
                ])
            })?,
            owned_vector(&self.attachment_manifests, FileRef::logical_owned_bytes)?,
        ])
    }
    fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema {
        crate::executor::json::JsonValueSchema::ForkReport
    }
}

impl bounded_io::UploadRecord for Uuid {
    fn logical_owned_bytes(&self) -> Result<u64> {
        Ok(0)
    }
}
impl bounded_io::UploadRecord for Capture {
    fn logical_owned_bytes(&self) -> Result<u64> {
        match self {
            Capture::Captured(resource) => crate::core::owned_sum([
                revision_owned_bytes(&resource.source)?,
                revision_owned_bytes(&resource.revision)?,
            ]),
            Capture::Unsupported(reason) => Ok(reason.capacity() as u64),
            Capture::InFlight(_) | Capture::Indeterminate(_) => Ok(0),
        }
    }
}
fn fork_collection_slot_bytes() -> u64 {
    use std::mem::size_of;
    [
        size_of::<ForkSelection>(),
        size_of::<Capture>(),
        size_of::<SharedGrant>(),
        size_of::<ReferenceGrant>(),
        size_of::<FileRef>(),
        size_of::<String>(),
        size_of::<(String, serde_json::Value)>(),
    ]
    .into_iter()
    .max()
    .unwrap_or(0) as u64
        + 2 * 5
            * size_of::<(
                serde::__private229::de::Content<'static>,
                serde::__private229::de::Content<'static>,
            )>() as u64
}

macro_rules! fork_preparation_records {
    ($($record:ty => $schema:ident => $slot:expr),+ $(,)?) => {$(
        impl bounded_io::PreparationRecord for $record {
            fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
                crate::core::owned_sum([
                    std::mem::size_of::<Self>() as u64,
                    bounded_io::UploadRecord::logical_owned_bytes(self)?,
                ]).map(Some)
            }
            fn maximum_slot_bytes() -> u64 { $slot }
            fn decode_owned_allowance(
                shape: crate::executor::json::JsonValueShape, scratch: u64, input: u64,
                code: Option<crate::wire::ErrorCode>,
            ) -> Result<u64> {
                let nodes = shape.lexical_items().checked_add(shape.virtual_items());
                let nodes = nodes.map(|nodes| if shape.unresolved_raw() { nodes.max(input) } else { nodes });
                // Input is held by the caller; admit only DTO slots, strings and decoder scratch.
                nodes.and_then(|nodes| nodes.checked_mul(Self::maximum_slot_bytes()))
                    .and_then(|slots| slots.checked_mul(8))
                    .and_then(|slots| input.checked_mul(2).and_then(|strings| slots.checked_add(strings)))
                    .and_then(|bytes| bytes.checked_add(scratch))
                    .and_then(|bytes| bytes.checked_add(std::mem::size_of::<Self>() as u64))
                    .ok_or_else(|| {
                        #[cfg(not(target_arch = "wasm32"))]
                        if let Some(code) = code {
                            return Error::NativeBoundary { code, reason: "fork decode storage overflows" };
                        }
                        let _ = code;
                        Error::Invalid("fork decode storage overflows".into())
                    })
            }
            fn json_value_schema() -> &'static crate::executor::json::JsonValueSchema { crate::executor::json::JsonValueSchema::$schema }
        }
    )+};
}
fork_preparation_records!(
    ForkRequest => ForkRequest => fork_collection_slot_bytes(),
    ForkReport => ForkReport => fork_collection_slot_bytes(),
    // Claim collections contain only strings; its one tagged owner needs Content scratch.
    AllocationClaim => None => (std::mem::size_of::<String>()
        + 2 * std::mem::size_of::<(serde::__private229::de::Content<'static>, serde::__private229::de::Content<'static>)>()) as u64,
    Capture => None => fork_collection_slot_bytes(),
    [u8; 32] => None => 0,
    Uuid => None => 0,
);

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Claims exact child-volume identities for a custom parent-authorized preparation path.
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
            .find_map(|resource| match &resource.source {
                ResourceRevision::Project { volume, .. } => Some(volume),
                _ => None,
            })
            .ok_or_else(|| Error::Invalid("fork has no source project".into()))?;
        let controller =
            ParentProjectController::new(self, parent, verifier, scope, source_project, None)?;
        controller.require(capability::FORK_PUBLISH, VolumeOperation::Read, None)?;
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
            let selected =
                if seed.inherited_through_sequence < conversation.resident_after_sequence() {
                    if seed.history_witness != Some(verifier.fork_history_proof(seed, &mut None)?) {
                        return Err(Error::Unauthorized(
                            "archived fork activation requires its original witness".into(),
                        ));
                    }
                    actual.format_version == 3
                        && actual.parent == seed.parent
                        && actual.parent_revision == seed.parent_revision
                        && actual.parent_agent == parent_agent
                        && actual.through_sequence == seed.inherited_through_sequence
                        && actual.attached_agents == seed.attached_agents
                } else {
                    actual
                        == InheritedConversationPrefix::select(
                            seed.parent.clone(),
                            seed.parent_revision,
                            seed.inherited_through_sequence,
                            &seed.attached_agents,
                            conversation,
                            &mut None,
                        )?
                };
            if !selected
                || crate::contract::canonical_json_bytes(&actual)?.as_slice() != bytes.as_ref()
            {
                return Err(Error::Conflict(
                    "inherited conversation differs from the selected parent prefix".into(),
                ));
            }
        }
        let digest = seed_digest(seed, MAX_REPORT_BYTES, None)?;
        if source_project.provider() != &self.provider
            || seed_creation_volumes(seed)?.any(|volume| volume.provider() != &self.provider)
        {
            return Err(Error::Invalid(
                "fork allocation belongs to another Filesystem provider".into(),
            ));
        }
        for volume in seed_creation_volumes(seed)? {
            self.claim_fork_allocation(
                AllocationClaim {
                    operation_id: seed.operation_id,
                    preparation_digest: digest,
                    task_origin: None,
                    parent: seed.parent.clone(),
                    child: seed.child.clone(),
                    volume: volume.clone(),
                },
                MAX_REQUEST_BYTES,
                None,
                None,
            )
            .await?;
        }
        self.bind_fork_seed(seed, MAX_REQUEST_BYTES, None, None)
            .await
    }

    /// Reads the original protected claim through the same bounded codec as publication verification.
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn fork_allocation<'a, P: StreamProvider>(
        &'a self,
        witness: &'a crate::durable_host::TaskAdmissionReadonlyWitness<P>,
        query: &'a Arc<crate::runtime::TaskContext>,
        admitted: &'a crate::resource_preparation::ResourcePreparationRequest,
        volume: &'a VolumeRef,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        crate::NativeFutureBundle<
            'a,
            Option<(
                AllocationClaim,
                crate::resource_preparation::ResourcePreparationRequest,
                acyclic_fs::NativeAllocation,
            )>,
        >,
    > {
        let original = witness.original_preparation_request_with_native_work(query, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let (request, request_custody) = original.await?;
            io.absorb_native_custody(request_custody)?;
            let reject = |reason| Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason,
            };
            if &request != admitted
                || request.provider != self.provider
                || request.creation_grant != capability::FORK_PUBLISH
            {
                return Err(reject(
                    "fork claim request differs from its original witnessed admission",
                ));
            }
            request.validate(&mut Some(&mut io.history_io()))?;
            let fork = io.value_decode_preparation::<ForkRequest>(
                &request.payload,
                crate::wire::ErrorCode::Invalid,
            )?;
            fork.0.validate(&mut Some(&mut io.history_io()))?;
            if fork.0.authorized_source != request.read_authorization
                || request.resources().count() != creation_volumes(&fork.0).count()
                || fork.0.preparation.maximum_inherited_bytes > request.maximum_bytes
            {
                return Err(reject(
                    "fork claim source or resources differ from their immutable request",
                ));
            }
            if !creation_volumes(&fork.0).any(|selected| selected == volume) {
                return Err(reject("fork claim volume is outside the witnessed request"));
            }
            for (resource, selected) in request.resources().zip(creation_volumes(&fork.0)) {
                if selected.provider() != &self.provider {
                    return Err(reject(
                        "fork claim resource belongs to another original provider",
                    ));
                }
                let before = io.owned_bytes()?;
                let (resource_hold, actual) = {
                    let actual =
                        bounded_io::content_workspace(&self.provider, selected, Some(&io))?
                            .into_resource();
                    let resource_hold = io.guard_owned(io.owned_bytes()? - before)?;
                    (resource_hold, actual)
                };
                if resource != &actual {
                    return Err(reject(
                        "fork claim resources differ from their original exact identities",
                    ));
                }
                drop(actual);
                drop(resource_hold);
            }
            let preparation_digest = record_digest(
                &fork.0,
                MAX_REQUEST_BYTES.min(request.maximum_bytes),
                Some(&io),
            )?;

            let (journal_hold, journal) = {
                let before = io.owned_bytes()?;
                let journal = allocation_ref(&self.provider, volume, Some(&io))?;
                let journal_hold = io.guard_owned(io.owned_bytes()? - before)?;
                (journal_hold, journal)
            };
            witness.verify_query_with_native_work(query, work).await?;
            let claim = read_allocation_claim(
                self,
                &journal,
                MAX_REQUEST_BYTES.min(request.maximum_bytes),
                Some(&io),
            )
            .await?;
            witness.verify_query_with_native_work(query, work).await?;
            let Some(claim) = claim else { return Ok(None) };
            let origin = claim
                .task_origin
                .as_ref()
                .ok_or_else(|| reject("fork claim has no genuine original preparation origin"))?;
            witness.verify_preparation_origin_with_native_work(query, &request, origin, work)?;
            if claim.operation_id != fork.0.operation_id
                || claim.parent != fork.0.parent
                || claim.child != fork.0.child
                || claim.volume != *volume
                || claim.preparation_digest != preparation_digest
            {
                return Err(reject(
                    "fork claim differs from its complete original witnessed request",
                ));
            }
            drop(journal);
            drop(journal_hold);
            drop(fork);
            let retained = io.owned_bytes()?;
            let custody = io.take_native_custody(retained)?;
            Ok(Some((claim, request, custody)))
        })
    }

    pub(crate) async fn read_fork_allocation<'a>(
        &self,
        seed: &ForkSeed,
        volume: &VolumeRef,
        binding: [u8; 32],
        maximum: u64,
        io: Option<&'a PreparationIo>,
    ) -> Result<(AllocationClaim, Option<bounded_io::DecodeReservation<'a>>)> {
        let (journal_hold, journal) = {
            let journal = allocation_ref(&self.provider, volume, io)?;
            let journal_hold = io
                .map(|io| io.guard_owned(journal.logical_owned_bytes()?))
                .transpose()?;
            (journal_hold, journal)
        };
        let (claim_hold, claim) = {
            let claim = read_allocation_claim(self, &journal, maximum, io)
                .await?
                .ok_or_else(|| Error::Unauthorized("fork child volume was not allocated".into()))?;
            let owned = bounded_io::UploadRecord::logical_owned_bytes(&claim)?;
            let claim_hold = io.map(|io| io.guard_owned(owned)).transpose()?;
            (claim_hold, claim)
        };
        if claim.operation_id != seed.operation_id
            || claim.parent != seed.parent
            || claim.child != seed.child
            || claim.volume != *volume
            || !seed_creation_volumes(seed)?.any(|selected| selected == volume)
        {
            return Err(Error::Conflict(
                "fork child volume belongs to another preparation".into(),
            ));
        }
        let retained =
            read_record_decoded_bounded(self, &journal, "/seed.json", maximum, io, |bytes, io| {
                match io {
                    Some(io) => io.decode_json::<[u8; 32]>(bytes),
                    None => crate::contract::json_from_slice(bytes)
                        .map_err(|cause| Error::Invalid(cause.to_string())),
                }
            })
            .await?
            .ok_or_else(|| {
                Error::Unauthorized("fork seed was not bound to its allocation".into())
            })?;
        if retained != binding {
            return Err(Error::Conflict(
                "fork seed differs from its allocated publication".into(),
            ));
        }
        drop(journal);
        drop(journal_hold);
        #[cfg(test)]
        acyclic_test_support::differential::record(("fork allocation verified", 1u64));
        Ok((claim, claim_hold))
    }

    pub(super) async fn bind_fork_seed(
        &self,
        seed: &ForkSeed,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
        preparation: Option<&Arc<TaskResourcePreparation>>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            preparation
                .ok_or(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native seed binding requires its original issued creation guard",
                })?
                .verify(
                    false,
                    Some(&crate::filesystem::PreparationIo::from_native_work(work)?),
                )?
                .await?;
        }
        let binding = seed_digest(seed, MAX_REPORT_BYTES, io)?;
        for volume in seed_creation_volumes(seed)? {
            let journal = allocation_ref(&self.provider, volume, io)?;
            let claim = read_record::<A, O, AllocationClaim>(
                self,
                &journal,
                "/claim.json",
                maximum_bytes,
                io,
            )
            .await?
            .ok_or_else(|| {
                stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "fork child volume was not allocated",
                )
            })?;
            if claim.operation_id != seed.operation_id
                || claim.parent != seed.parent
                || claim.child != seed.child
                || claim.volume != *volume
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "fork child volume belongs to another preparation",
                ));
            }
            let key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!("fork:{}:seed", seed.operation_id),
                    work,
                )
            })?)?;
            let prior = write_once(
                self,
                &journal,
                "/seed.json",
                &binding,
                maximum_bytes,
                &key,
                seed.operation_id,
                io,
                preparation,
            )
            .await?;
            if prior != binding {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "fork allocation has another seed",
                ));
            }
        }
        Ok(())
    }

    pub(super) async fn claim_fork_allocation(
        &self,
        claim: AllocationClaim,
        maximum_bytes: u64,
        io: Option<&PreparationIo>,
        preparation: Option<&Arc<TaskResourcePreparation>>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| claim.volume.validate(work))?;
        if claim.volume.provider() != &self.provider {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "fork allocation belongs to another Filesystem provider",
            ));
        }
        let journal = allocation_ref(&self.provider, &claim.volume, io)?;
        let name = std::str::from_utf8(journal.as_resource().key()).map_err(|_| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "allocation journal name is not UTF-8",
            )
        })?;
        let creation_key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!("fork:{}:allocation-journal", claim.operation_id),
                work,
            )
        })?)?;
        bounded_io::create(self, name, &creation_key, io, preparation).await?;
        let key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!("fork:{}:allocation", claim.operation_id),
                work,
            )
        })?)?;
        let prior = write_once(
            self,
            &journal,
            "/claim.json",
            &claim,
            maximum_bytes,
            &key,
            claim.operation_id,
            io,
            preparation,
        )
        .await?;
        if !same_allocation_claim(&prior, &claim) {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "fork child volume is already allocated",
            ));
        }
        Ok(())
    }
}

pub(super) fn same_allocation_claim(
    retained: &AllocationClaim,
    requested: &AllocationClaim,
) -> bool {
    if retained.operation_id != requested.operation_id
        || retained.preparation_digest != requested.preparation_digest
        || retained.parent != requested.parent
        || retained.child != requested.child
        || retained.volume != requested.volume
    {
        return false;
    }
    same_task_origin(
        retained.task_origin.as_ref(),
        requested.task_origin.as_ref(),
    )
}

pub(super) fn same_task_origin(
    retained: Option<&ResourceCreationOrigin>,
    requested: Option<&ResourceCreationOrigin>,
) -> bool {
    match (retained, requested) {
        (None, None) => true,
        (Some(prior), Some(current)) => {
            prior.owner == current.owner
                && prior.parent_task == current.parent_task
                && prior.preparation_task == current.preparation_task
                && prior.intended_child == current.intended_child
                && prior.admission_digest == current.admission_digest
                && prior.request_digest == current.request_digest
                && prior.source_grants == current.source_grants
        }
        _ => false,
    }
}

use crate::executor::stock_boundary;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fork_request_decodes_within_original_sixteen_mebibyte_budget() -> Result<()> {
        let bytes = include_bytes!("../../../fixtures/v2/fork-request.json");
        let io = PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(100_000, 16 * 1024 * 1024),
            &acyclic_fs::CancellationToken::new(),
        )?;
        let input = io.reserve_decode(bytes.len() as u64)?;
        let (request, held) = io.decode(
            std::marker::PhantomData::<ForkRequest>,
            bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        request.validate(&mut None)?;
        let owned = bounded_io::UploadRecord::logical_owned_bytes(&request)?
            + std::mem::size_of::<ForkRequest>() as u64;
        assert_eq!(held.bytes(), owned);
        assert_eq!(io.owned_bytes()?, owned + bytes.len() as u64);
        let claim = AllocationClaim {
            operation_id: request.operation_id,
            preparation_digest: [7; 32],
            task_origin: Some(ResourceCreationOrigin {
                owner: request.parent.clone(),
                parent_task: crate::TaskId::from_bytes([1; 16]),
                preparation_task: crate::TaskId::from_bytes([2; 16]),
                intended_child: request.operation_id,
                admission_digest: [3; 32],
                source_grants: crate::Capabilities::new(["history:read", "fork:publish"]),
                request_digest: [4; 32],
                creation_fence: crate::scheduler::LeaseFence {
                    reservation_id: "allocation-owner".into(),
                    placement: "worker".into(),
                },
            }),
            parent: request.parent.clone(),
            child: request.child.clone(),
            volume: request.preparation.child_private_volume.clone(),
        };
        drop(request);
        drop(held);
        drop(input);
        let bytes =
            serde_json::to_vec(&claim).map_err(|error| Error::Invalid(error.to_string()))?;
        let live = io.reserve_decode(14 * 1024 * 1024)?;
        let input = io.reserve_decode(bytes.capacity() as u64)?;
        let (decoded, held) = io.decode(
            std::marker::PhantomData::<AllocationClaim>,
            &bytes,
            crate::wire::ErrorCode::Storage,
        )?;
        assert_eq!(decoded, claim);
        assert_eq!(
            held.bytes(),
            bounded_io::UploadRecord::logical_owned_bytes(&decoded)?
                + std::mem::size_of::<AllocationClaim>() as u64
        );
        drop(decoded);
        drop(held);
        drop(bytes);
        drop(input);
        drop(live);
        assert_eq!(io.owned_bytes()?, 0);
        Ok(())
    }
}
