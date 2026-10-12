use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(super) fn record_removed(&self, path: &MountPath, file_id: FileId) -> Result<(), MountSourceError> {
        let text = self.path(path)?;
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        if removals.paths.insert(text, file_id) != Some(file_id) {
            removals.epoch = removals.epoch.wrapping_add(1);
            self.cursors.clear();
        }
        self.authored.record_projection_change(&ViewChange::Unbound(
            &self.authored.namespace_path(path)?,
            file_id,
        ));
        Ok(())
    }

pub(super) fn record_removed_identity(
        &self,
        record: FileRecord,
        metadata: FileMetadata,
        file: Arc<CheckoutDetachedFile<A, O>>,
    ) -> Result<(), MountSourceError> {
        self.detached
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .entry(record.file_id)
            .or_insert(DetachedIdentity {
                file,
                ready: true,
                published_epoch: 0,
                pending_epoch: None,
            });
        self.removals
            .lock()
            .map_err(|_| MountSourceError::Stale)?
            .records
            .insert(record.file_id, (record, metadata));
        self.authored
            .record_projection_change(&ViewChange::Node(record.file_id));
        Ok(())
    }

pub(super) fn record_rebound(&self, path: &str) -> Result<(), MountSourceError> {
        let mut removals = self.removals.lock().map_err(|_| MountSourceError::Stale)?;
        if removals.rebound(path) {
            self.cursors.clear();
        }
        Ok(())
    }
}

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
pub(crate) fn capture_host_paths_with_identity(
        &self,
        source_root: &Path,
        paths: &[MountPath],
        expected_root_identity: NativeRootIdentity,
    ) -> Result<(), MountSourceError> {
        if paths.is_empty() {
            return Ok(());
        }
        let _mutation = self.mutation_lease(None)?;
        for path in paths {
            self.promote_parents_locked(path)?;
            match self.promote_locked(path) {
                Ok(()) | Err(MountSourceError::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        self.authored
            .capture_host_paths_with_identity(source_root, paths, expected_root_identity)
    }

pub(super) fn mutation_lease(
        &self,
        expected: Option<u64>,
    ) -> Result<SourceMutationLease, MountSourceError> {
        self.wait(|| {
            let source_view = Arc::clone(&self.source_view);
            async move { source_view.write_stable(expected).await }
        })
    }
}
