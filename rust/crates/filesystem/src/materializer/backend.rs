use super::*;
use crate::native_host::HostRoot;
use std::sync::Arc;

/// Same-volume publisher bound to held checkout, target and backup roots.
pub struct NativeTreeMaterializationBackend {
    root: PathBuf,
    operation: PathBuf,
    pub(super) held_root: HostRoot,
    pub(super) held_target: HostRoot,
    pub(super) held_backup: HostRoot,
    operation_identity: [u8; 16],
    pub(super) work: NativeMaterializationWork,
    allocation: NativeAllocation,
}
impl NativeTreeMaterializationBackend {
    /// Binds existing checkout and staged recovery directories.
    pub fn new(
        root: PathBuf,
        operation: PathBuf,
        mut allocation: NativeAllocation,
        work: NativeMaterializationWork,
    ) -> Result<Arc<Self>, NativeTreeMaterializationError> {
        let capacity = root
            .capacity()
            .checked_add(operation.capacity())
            .ok_or(crate::WorkError::Overflow)?;
        if !work.shares_allocation_context(allocation.work())
            || allocation.bytes() < capacity as u64
        {
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
        let target = held_operation.open_dir_held(Path::new("target"), &mut &work)?;
        let target_identity = crate::native_host::directory_identity(&target, &mut &work)?;
        let backup = held_operation.open_dir_held(Path::new("backup"), &mut &work)?;
        let backup_identity = crate::native_host::directory_identity(&backup, &mut &work)?;
        Ok(Arc::new(Self {
            root,
            operation,
            held_root,
            held_target: HostRoot {
                directory: target,
                identity: target_identity,
            },
            held_backup: HostRoot {
                directory: backup,
                identity: backup_identity,
            },
            operation_identity: held_operation.identity().to_bytes(),
            work,
            allocation: header,
        }))
    }
    /// Original accounting context.
    pub fn work(&self) -> &NativeMaterializationWork {
        &self.work
    }
    /// Original checkout name.
    pub fn root_path(&self) -> &Path {
        &self.root
    }
    /// Original operation-directory name.
    pub fn operation_directory_path(&self) -> &Path {
        &self.operation
    }
    #[cfg(any(all(feature = "local", feature = "native-mount"), test))]
    pub(super) fn plan_paths(
        &self,
        operation_id: OperationId,
        from: GenerationId,
        to: GenerationId,
        paths: impl IntoIterator<Item = String>,
    ) -> Result<MaterializationPlan, NativeTreeMaterializationError> {
        let mut edits = Vec::new();
        for path in paths {
            validate_materialization_path(&path)
                .map_err(|()| NativeTreeMaterializationError::InvalidLayout)?;
            let edit = if native_entry_exists(&(&self.held_target, Path::new(&path)), &self.work)? {
                MaterializationEdit::Install {
                    path,
                    image: vec![1],
                }
            } else {
                MaterializationEdit::Remove { path }
            };
            edits.push(edit);
        }
        let plan = MaterializationPlan {
            operation_id,
            intent: MaterializationIntent::GenerationTransition { from, to },
            edits,
        };
        if !validate_plan(
            &plan,
            Vec::with_capacity(plan.edits.len().saturating_mul(2)),
            || Ok::<_, ()>(()),
        )
        .unwrap_or(false)
        {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(plan)
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
}
#[cfg(not(target_arch = "wasm32"))]
impl MaterializationBackend for Arc<NativeTreeMaterializationBackend> {
    type Error = NativeTreeMaterializationError;

    fn native_work<T>(
        &self,
        operation: impl FnOnce(&NativeMaterializationWork) -> Result<T, NativeTreeMaterializationError>,
    ) -> Result<Option<T>, Self::Error> {
        operation(&self.work).map(Some)
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
        let target = operation.open_dir_held(Path::new("target"), &mut &self.work)?;
        let backup = operation.open_dir_held(Path::new("backup"), &mut &self.work)?;
        Ok(Some(MaterializationPreparationIdentity {
            operation: operation.identity().to_bytes(),
            target: crate::native_host::directory_identity(&target, &mut &self.work)?.to_bytes(),
            backup: crate::native_host::directory_identity(&backup, &mut &self.work)?.to_bytes(),
        }))
    }
    async fn settle_observed(&self, edit: &MaterializationEdit) -> Result<(), Self::Error> {
        let backend = self.clone();
        let mut allocation = self.work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let edit = duplicate_edit(edit, &mut allocation)?;
        acyclic_native_runtime::run_blocking_io(
            move || {
                let _allocation = allocation;
                let path = Path::new(edit_path(&edit));
                for root in [
                    &backend.held_root,
                    &backend.held_target,
                    &backend.held_backup,
                ] {
                    let parent = root.open_dir_held(
                        path.parent()
                            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
                        &mut &backend.work,
                    )?;
                    crate::native_host::sync_directory_held(&parent, &mut &backend.work)?;
                }
                Ok::<_, NativeTreeMaterializationError>(())
            },
            Some((self.work.runtime_meter(), true)),
        )
        .map_err(|error| self.work.runtime_error(error))?
        .await?
    }

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        let backend = self.clone();
        let mut input = self.work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let edit = duplicate_edit(edit, &mut input)?;
        acyclic_native_runtime::run_blocking_io(
            move || {
                let _input = input;
                let edit = &edit;
                let path = edit_path(edit);
                let (live, target, backup) = backend.paths(path);
                if native_entry_exists(&backup, &backend.work)? {
                    return Err(NativeTreeMaterializationError::UnexpectedBackup);
                }
                let before = native_entry_fingerprint_for_edit(&live, edit, &backend.work)?;
                if before.is_some() && matches!(edit, MaterializationEdit::Install {image, ..} if image.starts_with(NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)) {
                    return Err(NativeTreeMaterializationError::ExternalMutation);
                }
                let after = match edit {
                    MaterializationEdit::Install { .. } => {
                        native_entry_fingerprint(&target, &backend.work)?
                            .ok_or_else(|| NativeTreeMaterializationError::MissingEntry)?
                            .into()
                    }
                    MaterializationEdit::Remove { .. } => None,
                    MaterializationEdit::SetMetadata { image, .. } => {
                        let desired = decode_native_metadata(image)?;
                        native_metadata_fingerprint(&live, Some(&desired), &backend.work)?
                    }
                    MaterializationEdit::Rename { .. } => {
                        return Err(NativeTreeMaterializationError::UnsupportedRename);
                    }
                };
                let metadata = if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                    Some(native_metadata(&live.0.stat(live.1, &mut &backend.work)?))
                } else {
                    None
                };
                let image = encode_native_witness(before, after, metadata, &backend.work)?;
                Ok(MaterializationPreimage {
                    image: image.0,
                    custody: Some(image.1),
                })
            },
            Some((self.work.runtime_meter(), true)),
        )
        .map_err(|error| self.work.runtime_error(error))?
        .await?
    }

    async fn observe(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<MaterializationObservation, Self::Error> {
        let backend = self.clone();
        let mut input = self.work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let edit = duplicate_edit(edit, &mut input)?;
        let preimage = MaterializationPreimage {
            image: copy_image(&preimage.image, &mut input)?,
            custody: None,
        };
        acyclic_native_runtime::run_blocking_io(
            move || {
                let _input = input;
                let edit = &edit;
                let preimage = &preimage;
                let (before, after) = decode_native_witness(&preimage.image)?;
                let path = edit_path(edit);
                let (live, target, backup) = backend.paths(path);
                let current = native_entry_fingerprint_for_edit(&live, edit, &backend.work)?;
                let target_current = if matches!(edit, MaterializationEdit::Install { .. }) {
                    native_entry_fingerprint(&target, &backend.work)?
                } else {
                    None
                };
                let backup_current = native_entry_fingerprint(&backup, &backend.work)?;
                let backup_exists = backup_current.is_some();
                if current == before
                    && current == after
                    && target_current.is_none()
                    && backup_exists
                {
                    return Ok(MaterializationObservation::Postimage);
                }
                if current == before {
                    if matches!(edit, MaterializationEdit::Install { .. })
                        && target_current != after
                    {
                        return Ok(MaterializationObservation::Diverged);
                    }
                    return Ok(MaterializationObservation::Preimage);
                }
                if current == after {
                    if target_current.is_some() && target_current != after {
                        return Ok(MaterializationObservation::Diverged);
                    }
                    return Ok(MaterializationObservation::Postimage);
                }

                // A crash may land between the two renames used for an install. The
                // durable backup proves the old binding was moved by this operation;
                // the still-present staged target means forward application can resume.
                if current.is_none()
                    && backup_current == before
                    && matches!(edit, MaterializationEdit::Install { .. })
                {
                    return if target_current.is_none() || target_current == after {
                        Ok(MaterializationObservation::Interrupted)
                    } else {
                        Ok(MaterializationObservation::Diverged)
                    };
                }
                Ok(MaterializationObservation::Diverged)
            },
            Some((self.work.runtime_meter(), true)),
        )
        .map_err(|error| self.work.runtime_error(error))?
        .await?
    }

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let backend = self.clone();
        let mut input = self.work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let edit = duplicate_edit(edit, &mut input)?;
        let preimage = MaterializationPreimage {
            image: copy_image(&preimage.image, &mut input)?,
            custody: None,
        };
        acyclic_native_runtime::run_blocking_io(
            move || {
                let _input = input;
                let edit = &edit;
                let preimage = &preimage;
                let path = edit_path(edit);
                let (live, target, backup) = backend.paths(path);
                let (before, after) = decode_native_witness(&preimage.image)?;
                if let MaterializationEdit::SetMetadata { image, .. } = edit {
                    let current = native_entry_fingerprint_for_edit(&live, edit, &backend.work)?;
                    if current == after {
                        return Ok(());
                    }
                    if current != before {
                        return Err(NativeTreeMaterializationError::ExternalMutation);
                    }
                    return apply_native_metadata(
                        &live,
                        &decode_native_metadata(image)?,
                        before,
                        &backend.work,
                    );
                }
                let target_expected = matches!(edit, MaterializationEdit::Install { .. });
                let backup_exists = native_entry_exists(&backup, &backend.work)?;
                let target_fingerprint = native_entry_fingerprint(&target, &backend.work)?;
                let target_exists = target_fingerprint.is_some();
                let live_fingerprint = native_entry_fingerprint(&live, &backend.work)?;
                let live_exists = live_fingerprint.is_some();
                if !backup_exists && !target_exists && live_fingerprint == after {
                    return Ok(());
                }
                if backup_exists && !target_exists && live_fingerprint == after {
                    return Ok(());
                }
                if backup_exists && live_fingerprint.is_some() && live_fingerprint != after {
                    return Err(NativeTreeMaterializationError::ExternalMutation);
                }
                if !backup_exists && live_fingerprint != before {
                    return Err(NativeTreeMaterializationError::ExternalMutation);
                }
                if target_expected && target_fingerprint != after {
                    return Err(NativeTreeMaterializationError::ExternalMutation);
                }
                if backup_exists {
                    if target_expected && target_exists {
                        remove_native_entry_durable(&live, &backend.work)?;
                        durable_native_rename(&target, &live, &backend.work)?;
                    } else if target_expected && !live_exists {
                        return Err(NativeTreeMaterializationError::MissingEntry);
                    } else if !target_expected {
                        remove_native_entry_durable(&live, &backend.work)?;
                    }
                    return Ok(());
                }
                if live_exists {
                    let parent = backup
                        .1
                        .parent()
                        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
                    crate::native_host::create_directories_held(
                        &backup.0.directory,
                        parent,
                        &mut &backend.work,
                    )?;
                    durable_native_rename(&live, &backup, &backend.work)?;
                }
                if target_expected {
                    if !target_exists {
                        return Err(NativeTreeMaterializationError::MissingEntry);
                    }
                    let parent = live
                        .1
                        .parent()
                        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
                    crate::native_host::create_directories_held(
                        &live.0.directory,
                        parent,
                        &mut &backend.work,
                    )?;
                    if let Err(error) = durable_native_rename(&target, &live, &backend.work) {
                        if native_entry_exists(&backup, &backend.work)?
                            && !native_entry_exists(&live, &backend.work)?
                        {
                            durable_native_rename(&backup, &live, &backend.work)?;
                        }
                        return Err(error);
                    }
                }
                Ok(())
            },
            Some((self.work.runtime_meter(), true)),
        )
        .map_err(|error| self.work.runtime_error(error))?
        .await?
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let backend = self.clone();
        let mut input = self.work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let edit = duplicate_edit(edit, &mut input)?;
        let preimage = MaterializationPreimage {
            image: copy_image(&preimage.image, &mut input)?,
            custody: None,
        };
        acyclic_native_runtime::run_blocking_io(
            move || {
                let _input = input;
                let edit = &edit;
                let preimage = &preimage;
                let path = edit_path(edit);
                let (live, _, backup) = backend.paths(path);
                let (before, after) = decode_native_witness(&preimage.image)?;
                let current = native_entry_fingerprint_for_edit(&live, edit, &backend.work)?;
                if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                    if current == before {
                        return Ok(());
                    }
                    if current != after {
                        return Err(NativeTreeMaterializationError::ExternalMutation);
                    }
                    let metadata = decode_native_preimage_metadata(&preimage.image)?
                        .ok_or(NativeTreeMaterializationError::InvalidLayout)?;
                    return apply_native_metadata(&live, &metadata, after, &backend.work);
                }
                let backup_current = native_entry_fingerprint(&backup, &backend.work)?;
                let backup_exists = backup_current.is_some();
                if backup_exists && backup_current != before {
                    return Err(NativeTreeMaterializationError::ExternalMutation);
                }
                if current == before && !backup_exists {
                    return Ok(());
                }
                if current != after && !(current.is_none() && backup_exists) {
                    return Err(NativeTreeMaterializationError::ExternalMutation);
                }
                match before {
                    None => remove_native_entry_durable(&live, &backend.work),
                    Some(_) if backup_exists => {
                        remove_native_entry_durable(&live, &backend.work)?;
                        durable_native_rename(&backup, &live, &backend.work)
                    }
                    Some(expected) if current == Some(expected) => Ok(()),
                    Some(_) => Err(NativeTreeMaterializationError::MissingEntry),
                }
            },
            Some((self.work.runtime_meter(), true)),
        )
        .map_err(|error| self.work.runtime_error(error))?
        .await?
    }
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
    crate::native_host::rename_no_replace_held(
        &source,
        Path::new(
            from.1
                .file_name()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        ),
        &target,
        Path::new(
            to.1.file_name()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        ),
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
    let parent = entry.0.open_dir_held(
        entry
            .1
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        &mut &*work,
    )?;
    let identity = crate::native_host::directory_identity(&parent, &mut &*work)?;
    let parent = HostRoot {
        directory: parent,
        identity,
    };
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
pub(super) fn edit_path(edit: &MaterializationEdit) -> &str {
    match edit {
        MaterializationEdit::Install { path, .. }
        | MaterializationEdit::Remove { path }
        | MaterializationEdit::SetMetadata { path, .. } => path,
        MaterializationEdit::Rename { from, .. } => from,
    }
}
pub(super) fn native_entry_exists(
    entry: &(&HostRoot, &Path),
    work: &NativeMaterializationWork,
) -> Result<bool, NativeTreeMaterializationError> {
    match entry.0.stat(entry.1, &mut &*work) {
        Ok(_) => Ok(true),
        Err(NativeTreeMaterializationError::Io(error))
            if error.kind() == std::io::ErrorKind::NotFound =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}
