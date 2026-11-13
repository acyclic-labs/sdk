//! Run input selection uses the ordinary admitted file binding.
use super::*;
use crate::{client_files::FileRoot, supervision::RunAdmissionSource};

impl<P: AuthorityProvider + ?Sized + 'static, O: AsyncObjectStore + 'static> Bound<P, O> {
    /// Select the original run inputs by their protected publication receipt.
    /// Current writable goal/policy paths cannot replace this admission.
    pub async fn run_admission(
        &self,
        agent: &AgentPath,
        operation: OperationId,
    ) -> Result<Option<RunAdmissionSource>, Error> {
        let (volume, scope, verifier) = self.file_volume(agent, FileRoot::Home {}, false).await?;
        self.volumes
            .original_run_admission(&volume, &scope, &verifier, operation, self.maximum_bytes)
            .await
            .map_err(turn)
    }
}
