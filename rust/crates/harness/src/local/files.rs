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

    /// Bind the selected checkout's retained baseline and exact immutable query.
    /// Waiting for its baseline file does not capture or mutate the checkout.
    #[cfg(feature = "native-execution")]
    pub async fn project_diff(
        &self,
        budget: acyclic_fs::WorkBudget,
        maximum: u32,
    ) -> Result<(
        Arc<dyn crate::conversation::ContentResidencyVerifier>,
        VolumeRef,
        acyclic_stream::BoxProviderStream<'static, Result<crate::context::DiffPublication>>,
    )> {
        use crate::task_volume::Query;
        use futures::StreamExt as _;
        let workspace = self
            .workspace
            .as_ref()
            .ok_or_else(|| Error::Unsupported("project diff requires a checkout".into()))?;
        let (selected, _) = workspace.selected_view().await?;
        let selected = selected.as_ref().unwrap_or_else(|| workspace.view());
        let key = super::checkout_tools::diff::key(selected.workspace().id())?;
        let mut observation = self
            .task_files
            .observe(Query::File(key.clone()), None, 1)
            .await?;
        let mut baseline = observation
            .snapshot
            .take()
            .and_then(|snapshot| snapshot.files.into_iter().next());
        while baseline.is_none() {
            let change = observation
                .changes
                .next()
                .await
                .ok_or_else(|| Error::Conflict("diff baseline follow ended".into()))??;
            baseline = change
                .file
                .into_iter()
                .chain(change.files)
                .find(|file| file.key == key);
        }
        let base = serde_json::from_slice(
            &baseline
                .ok_or_else(|| Error::Conflict("missing diff baseline".into()))?
                .bytes,
        )
        .map_err(storage)?;
        let reader = FilesystemContentVerifier::new(
            self.files
                .for_workspace(&self.project, selected.workspace().clone())?,
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )?;
        let stream = reader
            .file_query(&self.project, budget)
            .await?
            .observe_diff(base, maximum)
            .await?;
        Ok((reader, self.project.clone(), stream))
    }

    /// Read-only live queries over the agent's actual private SDK files.
    /// The existing verifier retains owner authority; execution-log files and
    /// the host journal remain distinct volumes.
    pub async fn scratch_files(
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
        .await
    }
}
