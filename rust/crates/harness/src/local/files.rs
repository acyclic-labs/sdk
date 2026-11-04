use super::*;
use crate::filesystem::FilesystemContentVerifier;

impl LocalHarness {
    /// Authority selecting this local agent's followed document and task files.
    pub fn client_owner(&self) -> crate::wire::Authority {
        crate::wire_codec::encode_authority(self.verifier.audience())
    }
    /// Authenticated reader shared by file-backed client input and documents.
    pub fn client_reader(&self) -> Result<Arc<dyn crate::conversation::ContentResidencyVerifier>> {
        Ok(self.signed_reader()?)
    }

    fn signed_reader(
        &self,
    ) -> Result<Arc<FilesystemContentVerifier<LocalAuthorityBackend, LocalObjectBackend>>> {
        FilesystemContentVerifier::new(
            Arc::clone(&self.files),
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )
    }

    /// One page of the selected checkout's changes since its first complete
    /// import, the fork base every agent edit is measured from. Without that
    /// complete baseline the query refuses to certify a diff. Projection, disclosure
    /// authority and paging are the shared [`crate::hosting::DiffPages`],
    /// which retains this checkout's projection in `diffs`.
    #[cfg(any(test, feature = "native-execution"))]
    pub(super) async fn project_diff(
        &self,
        diffs: &crate::hosting::DiffPages,
        page: &acyclic_harness_client::AgentPageRequest,
    ) -> Result<Vec<crate::context::DiffFilePage>> {
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("project diff requires a checkout".into()))?;
        let (selected, pending) = workspace.selected_view().await?;
        if let Some((operation, _)) = pending {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                operation.into_bytes(),
            )));
        }
        let selected = selected
            .as_ref()
            .unwrap_or_else(|| workspace.view())
            .workspace();
        let cancellation = acyclic_fs::CancellationToken::new();
        let base = selected
            .operation_generation(
                acyclic_fs::IdempotencyKey::from_bytes(
                    super::checkout_tools::capture::baseline_operation(selected.id()).into_bytes(),
                ),
                acyclic_fs::WorkBudget::UNBOUNDED,
                &cancellation,
            )
            .await
            .map_err(storage)?
            .value
            .ok_or_else(|| {
                Error::Unsupported("workspace diff requires a complete source baseline".into())
            })?
            .id();
        let current = selected
            .head(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .await
            .map_err(storage)?
            .value
            .id();
        diffs
            .page(
                &*self.signed_reader()?,
                &self.project,
                (
                    &self.files.generation_ref_id(base, None)?,
                    &self.files.generation_ref_id(current, None)?,
                ),
                page,
            )
            .await
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
