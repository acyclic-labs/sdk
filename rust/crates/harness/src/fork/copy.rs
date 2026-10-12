//! copy fork protocol.
use super::*;
pub(crate) fn copy_seed(source: &ForkSeed, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<ForkSeed> {
    crate::core::copy_owned(source, quota, |source, work| {
        source.validate(work)?;
        crate::core::owned_seed(source)
    })
}

pub(in crate::fork) fn owned_request(request: &ForkRequest) -> Result<u64> {
    use crate::core::{owned_buffer, owned_sum, owned_vector, owned_revision};
    let preparation = &request.preparation;
    owned_sum([
        request.parent.id.capacity() as u64, request.child.id.capacity() as u64,
        request.authorized_source.as_ref().map(|source| source.logical_owned_bytes()).transpose()?.unwrap_or(0),
        owned_buffer::<AgentId>(request.attached_agents.capacity())?,
        owned_vector(&request.selections, |selection| owned_revision(&selection.revision))?,
        preparation.child_project_volume.logical_owned_bytes()?,
        preparation.child_private_volume.logical_owned_bytes()?,
        preparation.child_result_volume.as_ref().map(VolumeRef::logical_owned_bytes).transpose()?.unwrap_or(0),
        preparation.summary.as_ref().map(|summary| summary.checkpoint.logical_owned_bytes()).transpose()?.unwrap_or(0),
        request.boundary.as_ref().map(|boundary| owned_sum([
            boundary.provider.logical_owned_bytes()?, boundary.evidence.capacity() as u64
        ])).transpose()?.unwrap_or(0),
    ])
}

pub(in crate::fork) fn copy_request(source: &ForkRequest, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<ForkRequest> {
    crate::core::copy_owned(source, quota, |source, work| { source.validate(work)?; owned_request(source) })
}

pub(in crate::fork) fn copy_revision(source: &ResourceRevision, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<ResourceRevision> {
    crate::core::copy_owned(source, quota, |source, work| { source.validate(work)?; crate::core::owned_revision(source) })
}

pub(in crate::fork) fn copy_report(source: &ForkReport, quota: &mut crate::wire_codec::CodecQuota<'_, '_>) -> Result<ForkReport> {
    crate::core::copy_owned(source, quota, |source, work| {
        use crate::core::{owned_sum, owned_vector, owned_buffer, owned_capture};
        source.validate(work)?;
        owned_sum([
            owned_request(&source.request)?,
            owned_vector(&source.captures, owned_capture)?,
            source.child_private_volume.logical_owned_bytes()?,
            source.child_private_generation.as_resource().logical_owned_bytes()?,
            source.child_result_volume.as_ref().map(VolumeRef::logical_owned_bytes).transpose()?.unwrap_or(0),
            source.child_result_generation.as_ref().map(|reference| reference.as_resource().logical_owned_bytes()).transpose()?.unwrap_or(0),
            owned_vector(&source.inherited_context, FileRef::logical_owned_bytes)?,
            owned_vector(&source.attachment_manifests, FileRef::logical_owned_bytes)?,
            owned_vector(&source.shared_grants, |grant| owned_sum([
                grant.volume.logical_owned_bytes()?, owned_buffer::<VolumeOperation>(grant.operations.len())?
            ]))?,
            owned_vector(&source.reference_grants, |grant| owned_sum([
                grant.file.logical_owned_bytes()?,
                grant.attachment_manifest.as_ref().map(FileRef::logical_owned_bytes).transpose()?.unwrap_or(0)
            ]))?,
            source.summary.as_ref().map(|summary| owned_sum([
                summary.selection.checkpoint.logical_owned_bytes()?, summary.context.logical_owned_bytes()?,
                owned_vector(&summary.payloads, FileRef::logical_owned_bytes)?,
                owned_vector(&summary.references, FileRef::logical_owned_bytes)?,
            ])).transpose()?.unwrap_or(0),
        ])
    })
}
