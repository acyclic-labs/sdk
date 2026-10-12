//! Admission of resource creator captures.
use super::*;

#[cfg(feature = "filesystem")]
pub(crate) fn resource_creation_clone_reservation(
    request: &crate::resource_preparation::ResourcePreparationRequest,
    proof: &crate::resource_preparation::ResourceCreationProofRef,
    origin: &crate::resource_preparation::ResourceCreationOrigin,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    inspect_metadata(&(request, proof, origin), work)?;
    let bytes = owned_sum([
        request.logical_owned_bytes()?,
        proof.provider.logical_owned_bytes()?,
        proof.resource.logical_owned_bytes()?,
        proof.evidence.logical_owned_bytes()?,
        origin.owner.id.capacity() as u64,
        origin.source_grants.logical_owned_bytes()?,
    ])?;
    if let Some(io) = work.as_deref_mut() {
        io.retain_owned(bytes)?;
    }
    Ok(bytes)
}
