//! One immutable baseline file per selected workspace, under the existing guard.
use super::*;
use crate::task_volume::{Component, FileKey, FileValue, Query};

pub(in crate::local) fn key(workspace: acyclic_fs::WorkspaceId) -> Result<FileKey> {
    Ok(FileKey::Ui {
        directory: Component::new("diff")?,
        name: Component::new(format!(
            "{}.json",
            uuid::Uuid::from_bytes(workspace.into_bytes())
        ))?,
    })
}

impl CheckoutTools {
    pub(super) async fn retain_diff_base(
        &self,
        workspace: &acyclic_fs::Workspace<LocalAuthorityBackend, LocalObjectBackend>,
    ) -> Result<()> {
        let key = key(workspace.id())?;
        // Shared with ordinary UI writes: CAS retries only unrelated file updates.
        // There is no new synchronization primitive or nested checkout acquisition.
        loop {
            let snapshot = self
                .task_files
                .snapshot(&Query::File(key.clone()), 1)
                .await?;
            if !snapshot.files.is_empty() {
                return Ok(());
            }
            let generation = workspace
                .head(
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &acyclic_fs::CancellationToken::new(),
                )
                .await
                .map_err(storage)?
                .value;
            let bytes = serde_json::to_vec(&generation.id()).map_err(storage)?;
            match self
                .task_files
                .write(snapshot.generation, &FileValue::new(key.clone(), bytes))
                .await
            {
                Ok(_) => return Ok(()),
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(error.into()),
            }
        }
    }
}
