use super::*;
use crate::filesystem::{FilesystemContentVerifier, FilesystemFileQuery};

impl LocalHarness {
    /// Authority selecting this local agent's followed document and task files.
    pub fn client_owner(&self) -> crate::wire::Authority {
        crate::wire_codec::encode_authority(self.verifier.audience())
    }
    /// Authenticated reader shared by file-backed client input and documents.
    pub fn client_reader(&self) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        Ok(FilesystemContentVerifier::new(
            Arc::clone(&self.files),
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )?)
    }

    /// Publishes host-selected attachments in this agent's private volume.
    pub fn client_publisher(&self) -> Result<Arc<dyn crate::conversation::ContentPublisher>> {
        Ok(crate::filesystem::FilesystemContentPublisher::new(
            Arc::clone(&self.files),
            &self.scratch,
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )?)
    }

    /// Read-only live queries over the agent's actual private SDK files.
    /// The existing verifier retains owner authority; execution-log files and
    /// the host journal remain distinct volumes.
    pub fn scratch_files(
        &self,
        budget: acyclic_fs::WorkBudget,
    ) -> Result<FilesystemFileQuery<LocalStream, LocalObjectBackend>> {
        FilesystemContentVerifier::new(
            Arc::clone(&self.files),
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )?
        .file_query(&self.scratch, budget)
    }
}
