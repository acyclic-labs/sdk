use super::*;
#[cfg(feature = "filesystem")]
use crate::conversation::{ContentResidencyVerifier, FileRef};
#[cfg(feature = "filesystem")]
use std::sync::Arc;
/// Owner-consumed permissions and their authenticated content implementations.
pub struct CreatedResourceAccess {
    pub(crate) operation: OperationId,
    pub(crate) admission_digest: [u8; 32],
    pub(crate) grants: Capabilities,
    pub(crate) content: Vec<CreatedContentBinding>,
    #[cfg(feature = "filesystem")]
    pub(crate) references: Vec<(FileRef, Arc<dyn ContentResidencyVerifier>)>,
    pub(crate) proofs: Vec<ResourceCreationProofRef>,
    pub(crate) publication_lease: Option<std::sync::Arc<dyn CreatedTaskLease>>,
}
#[cfg(feature = "filesystem")]
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
}
