use super::*;
use crate::filesystem::FilesystemContentVerifier;

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

    /// Selected identity read from the existing persisted checkout state.
    #[cfg(feature = "native-execution")]
    pub async fn project_selection(&self) -> Result<acyclic_fs::WorkspaceId> {
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("project diff requires a checkout".into()))?;
        let (selected, _) = workspace.selected_view().await?;
        let selected = selected.as_ref().unwrap_or_else(|| workspace.view());
        Ok(selected.workspace().id())
    }

    /// Publisher of this harness's prompt inputs on its private journal volume.
    pub(super) fn input_publisher_with_limit(
        &self,
        maximum_bytes: u64,
    ) -> Result<Arc<FilesystemContentPublisher<LocalAuthorityBackend, LocalObjectBackend>>> {
        FilesystemContentPublisher::new(
            self.files.clone(),
            &self.journal,
            &self.verifier,
            &self.signed,
            maximum_bytes,
            None,
        )
    }
}
