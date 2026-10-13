use super::*;
use crate::conversation::{ContentResidencyVerifier, FileRef};
use std::{mem::size_of, sync::Arc};
/// Owner-consumed permissions and their authenticated content implementations.
pub struct CreatedResourceAccess {
    pub(crate) operation: OperationId,
    pub(crate) admission_digest: [u8; 32],
    pub(crate) grants: Capabilities,
    pub(crate) content: Vec<CreatedContentBinding>,
    pub(crate) references: Vec<(FileRef, Arc<dyn ContentResidencyVerifier>)>,
    pub(crate) proofs: Vec<ResourceCreationProofRef>,
    pub(crate) publication_lease: Option<std::sync::Arc<dyn CreatedTaskLease>>,
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(crate) native_custody: Option<acyclic_fs::NativeAllocation>,
}
impl CreatedResourceAccess {
    pub(crate) fn current_content(
        &self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<Option<&CreatedContentBinding>> {
        for binding in &self.content {
            charge(work, 1)?;
            if binding.default_writer {
                return Ok(Some(binding));
            }
        }
        Ok(None)
    }
    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        sum([
            size_of::<Self>() as u64,
            self.grants.logical_owned_bytes()?,
            vector_bytes(&self.content, |b| {
                sum([
                    b.volume.logical_owned_bytes()?,
                    b.authority.as_ref().map_or(0, |a| a.id.capacity() as u64),
                ])
            })?,
            vector_bytes(&self.references, |(file, _)| file.logical_owned_bytes())?,
            vector_bytes(&self.proofs, proof_bytes)?,
        ])
    }
}
