use super::*;
use crate::{
    filesystem::{FilesystemForkPreparer, PreparationIo},
    fork::{ForkReport, ForkRequest, ResourceRevision},
    resource_preparation::TaskResourcePreparation,
};
use acyclic_fs::facade::FsWork as _;

impl LocalWorkspace {
    pub(super) async fn prepare_child(
        &self,
        source: &workspace::View,
        cut: acyclic_fs::LazySnapshotRef,
        preparer: &FilesystemForkPreparer<LocalAuthorityBackend, LocalObjectBackend>,
        request: &ForkRequest,
        preparation: &Arc<TaskResourcePreparation>,
        io: &PreparationIo,
    ) -> Result<(crate::charged_box::ChargedBox<ForkReport>, workspace::View)> {
        let selected = request.selections.iter().find_map(|selection| {
            if let ResourceRevision::Project { volume, generation } = &selection.revision {
                Some((volume, generation))
            } else {
                None
            }
        });
        let (volume, generation) =
            selected.ok_or_else(|| Error::Invalid("child fork has no project selection".into()))?;
        let source_name = volume.storage_name(&mut Some(&mut io.history_io()))?;
        if source.workspace().id()
            != self
                .filesystem
                .workspace_id(&source_name)
                .map_err(storage)?
            || cut.workspace_id != source.workspace().id()
            || generation.as_resource().key() != cut.authored_generation.digest().as_bytes()
        {
            return Err(Error::Conflict(
                "child fork differs from its lazy source cut".into(),
            ));
        }
        let operation = crate::conversation::BoundedOperationFuture::new(Some(io), || {
            preparer.project_creation_operation(request, preparation, io)
        })?
        .await?;
        let destination = request
            .preparation
            .child_project_volume
            .storage_name(&mut Some(&mut io.history_io()))?;
        let mut accounting = io.accounting(io.cancellation());
        let selected = crate::conversation::BoundedOperationFuture::new(Some(io), || {
            source.prepare_fork_snapshot(&destination, cut, operation, &mut accounting)
        })?
        .await;
        let selected = io.sdk(io.finish(selected, &accounting))?;
        drop(accounting);
        let report = crate::conversation::BoundedOperationFuture::new(Some(io), || {
            preparer.prepare_for_task(request, preparation, Some(io))
        })?
        .await?;
        preparation.verify(false, Some(io))?.await?;
        let child = io.sdk(
            crate::conversation::BoundedOperationFuture::new(Some(io), || {
                self.filesystem
                    .open_workspace(&destination, io, io.cancellation())
            })?
            .await,
        )?;
        let original = io
            .sdk(
                crate::conversation::BoundedOperationFuture::new(Some(io), || {
                    self.filesystem.workspace_for_creation_operation(
                        &destination,
                        child.volume_config(),
                        operation,
                        io,
                        io.cancellation(),
                    )
                })?
                .await,
            )?
            .ok_or_else(|| Error::Conflict("child fork has no original creation".into()))?;
        let original_generation = original.id();
        drop(original);
        let mut accounting = io.accounting(io.cancellation());
        let bound = crate::conversation::BoundedOperationFuture::new(Some(io), || {
            source.bind_prepared_fork(
                selected,
                child,
                original_generation,
                operation,
                &mut accounting,
            )
        })?
        .await;
        let (view, _) = io.sdk(io.finish(bound, &accounting))?;
        let graph = acyclic_fs::WorkspaceGraph::new(self.store.clone());
        crate::conversation::BoundedOperationFuture::new(Some(io), || {
            graph.register_existing_child_at_generation(
                source.workspace(),
                view.workspace(),
                cut.authored_generation,
                original_generation,
            )
        })?
        .await
        .map_err(storage)?;
        Ok((report, view))
    }
}
