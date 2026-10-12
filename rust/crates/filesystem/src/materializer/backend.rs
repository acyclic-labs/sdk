use super::*;
use crate::native_host::HostRoot;
use std::sync::Arc;

/// Main's tree publisher bound to held checkout, target and backup roots.
pub struct NativeTreeMaterializationBackend {
    root: PathBuf,
    operation: PathBuf,
    pub(super) held_root: HostRoot,
    pub(super) held_target: HostRoot,
    pub(super) held_backup: HostRoot,
    operation_identity: [u8; 16],
    pub(super) work: NativeMaterializationWork,
    _allocation: NativeAllocation,
}
impl NativeTreeMaterializationBackend {
    /// Binds existing recovery directories without creating a parallel checkout.
    pub fn new(
        root: PathBuf,
        operation: PathBuf,
        mut allocation: NativeAllocation,
        work: NativeMaterializationWork,
    ) -> Result<Arc<Self>, NativeTreeMaterializationError> {
        let bytes = root
            .capacity()
            .checked_add(operation.capacity())
            .ok_or(crate::WorkError::Overflow)?;
        if !work.shares_allocation_context(allocation.work()) || allocation.bytes() < bytes as u64 {
            return Err(NativeTreeMaterializationError::MissingCustody);
        }
        let mut header = work.claim(
            std::mem::size_of::<Self>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        header.absorb(&mut allocation)?;
        let held_root = HostRoot::open(&root, &mut &work)?;
        let held_operation = HostRoot::open(&operation, &mut &work)?;
        validate_layout(&held_root, &held_operation, Path::new(""), &work)?;
        let held_target = held_child(&held_operation, Path::new("target"), &work)?;
        let held_backup = held_child(&held_operation, Path::new("backup"), &work)?;
        Ok(Arc::new(Self {
            root,
            operation,
            held_root,
            held_target,
            held_backup,
            operation_identity: held_operation.identity().to_bytes(),
            work,
            _allocation: header,
        }))
    }
    /// Existing accounting context.
    pub fn work(&self) -> &NativeMaterializationWork {
        &self.work
    }
    /// Original checkout name.
    pub fn root_path(&self) -> &Path {
        &self.root
    }
    /// Original operation directory name.
    pub fn operation_directory_path(&self) -> &Path {
        &self.operation
    }
    fn paths<'a>(
        &'a self,
        path: &'a str,
    ) -> (
        (&'a HostRoot, &'a Path),
        (&'a HostRoot, &'a Path),
        (&'a HostRoot, &'a Path),
    ) {
        let path = Path::new(path);
        (
            (&self.held_root, path),
            (&self.held_target, path),
            (&self.held_backup, path),
        )
    }
    async fn run<T: Send + 'static>(
        self: &Arc<Self>,
        edit: &MaterializationEdit,
        image: &[u8],
        operation: impl FnOnce(
            &Self,
            &MaterializationEdit,
            &[u8],
        ) -> Result<T, NativeTreeMaterializationError>
        + Send
        + 'static,
    ) -> Result<T, NativeTreeMaterializationError> {
        let mut allocation = self.work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let edit = duplicate_edit(edit, &mut allocation)?;
        let image = copy_image(image, &mut allocation)?;
        let backend = self.clone();
        acyclic_native_runtime::run_blocking_io(
            move || {
                let _allocation = allocation;
                operation(&backend, &edit, &image)
            },
            Some((self.work.runtime_meter(), true)),
        )
        .map_err(|error| self.work.runtime_error(error))?
        .await?
    }
}
impl MaterializationBackend for Arc<NativeTreeMaterializationBackend> {
    type Error = NativeTreeMaterializationError;
    fn native_work(&self) -> Option<&NativeMaterializationWork> {
        Some(&self.work)
    }
    fn allocation_identity(&self) -> Option<[u8; 16]> {
        Some(self.held_root.identity().to_bytes())
    }
    fn preparation_identity(&self) -> Option<MaterializationPreparationIdentity> {
        Some(MaterializationPreparationIdentity {
            operation: self.operation_identity,
            target: self.held_target.identity().to_bytes(),
            backup: self.held_backup.identity().to_bytes(),
        })
    }
    async fn observe_allocation_identity(&self) -> Result<Option<[u8; 16]>, Self::Error> {
        self.work
            .root_identity(&self.root)
            .map(|identity| Some(identity.to_bytes()))
    }
    async fn observe_preparation_identity(
        &self,
    ) -> Result<Option<MaterializationPreparationIdentity>, Self::Error> {
        let operation = HostRoot::open(&self.operation, &mut &self.work)?;
        Ok(Some(MaterializationPreparationIdentity {
            operation: operation.identity().to_bytes(),
            target: held_child(&operation, Path::new("target"), &self.work)?
                .identity()
                .to_bytes(),
            backup: held_child(&operation, Path::new("backup"), &self.work)?
                .identity()
                .to_bytes(),
        }))
    }
    async fn settle_observed(&self, edit: &MaterializationEdit) -> Result<(), Self::Error> {
        self.run(edit, &[], |backend, edit, _| {
            let parent = Path::new(edit_path(edit))
                .parent()
                .ok_or(Self::Error::InvalidLayout)?;
            for root in [
                &backend.held_root,
                &backend.held_target,
                &backend.held_backup,
            ] {
                let parent = root.open_dir_held(parent, &mut &backend.work)?;
                crate::native_host::sync_directory_held(&parent, &mut &backend.work)?;
            }
            Ok(())
        })
        .await
    }
    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        self.run(edit, &[], |backend, edit, _| {
            let (live, target, backup) = backend.paths(edit_path(edit));
            if host_stat(backup.0, backup.1, &backend.work)?.is_some() { return Err(Self::Error::UnexpectedBackup); }
            let before = native_entry_fingerprint_for_edit(&live, edit, &backend.work)?;
            if before.is_some() && matches!(edit, MaterializationEdit::Install { image, .. } if image.starts_with(NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)) { return Err(Self::Error::ExternalMutation); }
            let after = match edit {
                MaterializationEdit::Install { .. } => Some(native_entry_fingerprint(&target, &backend.work)?.ok_or(Self::Error::MissingEntry)?),
                MaterializationEdit::Remove { .. } => None,
                MaterializationEdit::SetMetadata { image, .. } => native_metadata_fingerprint(&live, Some(&decode_native_metadata(image)?), &backend.work)?,
                MaterializationEdit::Rename { .. } => return Err(Self::Error::UnsupportedRename),
            };
            let metadata = if matches!(edit, MaterializationEdit::SetMetadata { .. }) { Some(native_metadata(&live.0.stat(live.1, &mut &backend.work)?)) } else { None };
            let (image, allocation) = encode_native_witness(before, after, metadata, &backend.work)?;
            Ok(MaterializationPreimage { image, custody: Some(allocation) })
        }).await
    }
    async fn observe(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<MaterializationObservation, Self::Error> {
        self.run(edit, &preimage.image, |backend, edit, image| {
            let (before, after) = decode_native_witness(image)?;
            let (live, target, backup) = backend.paths(edit_path(edit));
            let current = native_entry_fingerprint_for_edit(&live, edit, &backend.work)?;
            let target = if matches!(edit, MaterializationEdit::Install { .. }) {
                native_entry_fingerprint(&target, &backend.work)?
            } else {
                None
            };
            let backup = native_entry_fingerprint(&backup, &backend.work)?;
            if current == before && current == after && target.is_none() && backup.is_some() {
                return Ok(MaterializationObservation::Postimage);
            }
            if current == before {
                return Ok(
                    if matches!(edit, MaterializationEdit::Install { .. }) && target != after {
                        MaterializationObservation::Diverged
                    } else {
                        MaterializationObservation::Preimage
                    },
                );
            }
            if current == after {
                return Ok(if target.is_some() && target != after {
                    MaterializationObservation::Diverged
                } else {
                    MaterializationObservation::Postimage
                });
            }
            if current.is_none()
                && backup == before
                && matches!(edit, MaterializationEdit::Install { .. })
            {
                return Ok(if target.is_none() || target == after {
                    MaterializationObservation::Interrupted
                } else {
                    MaterializationObservation::Diverged
                });
            }
            Ok(MaterializationObservation::Diverged)
        })
        .await
    }
    async fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        self.run(edit, &preimage.image, |backend, edit, image| {
            let (live, target, backup) = backend.paths(edit_path(edit));
            let (before, after) = decode_native_witness(image)?;
            if let MaterializationEdit::SetMetadata { image, .. } = edit {
                let current = native_entry_fingerprint_for_edit(&live, edit, &backend.work)?;
                if current == after {
                    return Ok(());
                }
                if current != before {
                    return Err(Self::Error::ExternalMutation);
                }
                return apply_native_metadata(
                    &live,
                    &decode_native_metadata(image)?,
                    before,
                    &backend.work,
                );
            }
            let install = matches!(edit, MaterializationEdit::Install { .. });
            let backup_exists = host_stat(backup.0, backup.1, &backend.work)?.is_some();
            let staged = native_entry_fingerprint(&target, &backend.work)?;
            let current = native_entry_fingerprint(&live, &backend.work)?;
            if staged.is_none() && current == after {
                return Ok(());
            }
            if backup_exists && current.is_some() && current != after
                || !backup_exists && current != before
                || install && staged != after
            {
                return Err(Self::Error::ExternalMutation);
            }
            if backup_exists {
                if install && staged.is_some() {
                    remove_native_entry_durable(&live, &backend.work)?;
                    return durable_native_rename(&target, &live, &backend.work);
                }
                if install && current.is_none() {
                    return Err(Self::Error::MissingEntry);
                }
                if !install {
                    remove_native_entry_durable(&live, &backend.work)?;
                }
                return Ok(());
            }
            if current.is_some() {
                create_parent(&backup, &backend.work)?;
                durable_native_rename(&live, &backup, &backend.work)?;
            }
            if install {
                if staged.is_none() {
                    return Err(Self::Error::MissingEntry);
                }
                create_parent(&live, &backend.work)?;
                if let Err(error) = durable_native_rename(&target, &live, &backend.work) {
                    if host_stat(backup.0, backup.1, &backend.work)?.is_some()
                        && host_stat(live.0, live.1, &backend.work)?.is_none()
                    {
                        durable_native_rename(&backup, &live, &backend.work)?;
                    }
                    return Err(error);
                }
            }
            Ok(())
        })
        .await
    }
    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        self.run(edit, &preimage.image, |backend, edit, image| {
            let (live, _, backup) = backend.paths(edit_path(edit));
            let (before, after) = decode_native_witness(image)?;
            let current = native_entry_fingerprint_for_edit(&live, edit, &backend.work)?;
            if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                if current == before {
                    return Ok(());
                }
                if current != after {
                    return Err(Self::Error::ExternalMutation);
                }
                return apply_native_metadata(
                    &live,
                    &decode_native_preimage_metadata(image)?.ok_or(Self::Error::InvalidLayout)?,
                    after,
                    &backend.work,
                );
            }
            let backup_image = native_entry_fingerprint(&backup, &backend.work)?;
            if backup_image.is_some() && backup_image != before {
                return Err(Self::Error::ExternalMutation);
            }
            if current == before && backup_image.is_none() {
                return Ok(());
            }
            if current != after && !(current.is_none() && backup_image.is_some()) {
                return Err(Self::Error::ExternalMutation);
            }
            match before {
                None => remove_native_entry_durable(&live, &backend.work),
                Some(_) if backup_image.is_some() => {
                    remove_native_entry_durable(&live, &backend.work)?;
                    durable_native_rename(&backup, &live, &backend.work)
                }
                Some(expected) if current == Some(expected) => Ok(()),
                Some(_) => Err(Self::Error::MissingEntry),
            }
        })
        .await
    }
}
pub(super) fn held_child(
    root: &HostRoot,
    path: &Path,
    work: &NativeMaterializationWork,
) -> Result<HostRoot, NativeTreeMaterializationError> {
    work.state(|state| root.open_child(path, state))
        .map_err(native_materialization_work_error)
}
fn create_parent(
    entry: &(&HostRoot, &Path),
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let parent = entry
        .1
        .parent()
        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
    work.state(|state| entry.0.create_dir_all_held(parent, state))
        .map(|_| ())
        .map_err(native_materialization_work_error)
}
pub(super) fn durable_native_rename(
    from: &(&HostRoot, &Path),
    to: &(&HostRoot, &Path),
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let source = from.0.open_dir_held(
        from.1
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        &mut &*work,
    )?;
    let target = to.0.open_dir_held(
        to.1.parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        &mut &*work,
    )?;
    let source_name = Path::new(
        from.1
            .file_name()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
    );
    let target_name = Path::new(
        to.1.file_name()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
    );
    crate::native_host::rename_no_replace_held(
        &source,
        source_name,
        &target,
        target_name,
        None,
        &mut &*work,
    )?;
    crate::native_host::sync_directory_held(&source, &mut &*work)?;
    crate::native_host::sync_directory_held(&target, &mut &*work)
}
pub(super) fn remove_native_entry_durable(
    entry: &(&HostRoot, &Path),
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let parent = held_child(
        entry.0,
        entry
            .1
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        work,
    )?;
    let leaf = Path::new(
        entry
            .1
            .file_name()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
    );
    work.with_original_leaf(|state| parent.remove(leaf, state))
        .map_err(native_materialization_work_error)?;
    crate::native_host::sync_directory_held(&parent.directory, &mut &*work)
}
pub(super) fn host_stat(
    root: &HostRoot,
    path: &Path,
    work: &NativeMaterializationWork,
) -> Result<Option<crate::native_host::HostStat>, NativeTreeMaterializationError> {
    match root.stat(path, &mut &*work) {
        Ok(stat) => Ok(Some(stat)),
        Err(NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}
pub(super) fn edit_path(edit: &MaterializationEdit) -> &str {
    match edit {
        MaterializationEdit::Install { path, .. }
        | MaterializationEdit::Remove { path }
        | MaterializationEdit::SetMetadata { path, .. } => path,
        MaterializationEdit::Rename { from, .. } => from,
    }
}
