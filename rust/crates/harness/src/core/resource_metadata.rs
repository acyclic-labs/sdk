//! Admission of resource creator captures.
use super::*;

#[cfg(feature = "filesystem")]
pub(crate) fn resource_creation_clone_reservation(
    request: &crate::resource_preparation::ResourcePreparationRequest,
    proof: &crate::resource_preparation::ResourceCreationProofRef,
    origin: &crate::resource_preparation::ResourceCreationOrigin,
    work: &mut Option<&mut dyn HistoryIo>,
) -> Result<u64> {
    let mut inventory = inventory::CloneInventory {
        bytes: Some(0),
        work: work.as_deref_mut().map(|io| io as &mut dyn HistoryIo),
        ..Default::default()
    };
    let result = (request, proof, origin).serialize(&mut inventory);
    if let Some(error) = inventory.failure.take() {
        return Err(error);
    }
    result.map_err(|_| Error::Invalid("closed creator capture exceeds inventory bounds".into()))?;
    let bytes = inventory.bytes.expect("size inventory");
    drop(inventory);
    if let Some(io) = work.as_deref_mut() {
        io.retain_owned(bytes)?;
    }
    Ok(bytes)
}
