use super::*;
use crate::native_host::HostRoot;
use std::sync::Arc;

/// Same-volume tree publisher bound to actual held allocations.
pub struct NativeTreeMaterializationBackend {
    root: PathBuf,
    operation: PathBuf,
    operation_identity: [u8; 16],
    pub(super) held_root: HostRoot,
    pub(super) held_target: HostRoot,
    pub(super) held_backup: HostRoot,
    #[cfg(feature = "native-mount")]
    execution: Option<Arc<NativeSourceExecutionView>>,
    allocation: NativeAllocation,
}
impl NativeTreeMaterializationBackend {
    /// Binds existing root, staged target and backup directories.
    pub fn new(
        root: PathBuf,
        operation: PathBuf,
        mut allocation: NativeAllocation,
        work: NativeMaterializationWork,
    ) -> Result<Arc<Self>, NativeTreeMaterializationError> {
        if !work.shares_allocation_context(allocation.work())
            || allocation.bytes()
                < root
                    .capacity()
                    .checked_add(operation.capacity())
                    .ok_or(crate::WorkError::Overflow)? as u64
        {
            return Err(NativeTreeMaterializationError::MissingCustody);
        }
        let mut header = work.claim(
            std::mem::size_of::<Self>() as u64,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        header.absorb(&mut allocation)?;
        let mut allocation = header;
        let held_root = HostRoot::open(&root, &mut &work)?;
        let held_operation = HostRoot::open(&operation, &mut &work)?;
        validate_layout(&held_root, &held_operation, Path::new(""), &work)?;
        let target = held_operation.open_dir_held(Path::new("target"), &mut &work)?;
        let identity = crate::native_host::directory_identity(&target, &mut &work)?;
        let held_target = HostRoot::from_held_dir_with_identity(target, identity);
        let backup = held_operation.open_dir_held(Path::new("backup"), &mut &work)?;
        let identity = crate::native_host::directory_identity(&backup, &mut &work)?;
        let held_backup = HostRoot::from_held_dir_with_identity(backup, identity);
        Ok(Arc::new(Self {
            root,
            operation,
            operation_identity: held_operation.identity().to_bytes(),
            held_root,
            held_target,
            held_backup,
            #[cfg(feature = "native-mount")]
            execution: work.execution.clone(),
            allocation,
        }))
    }
    /// Original accounting context retained by the backend.
    pub fn work(&self) -> NativeMaterializationWork {
        NativeMaterializationWork {
            #[cfg(feature = "native-mount")]
            execution: self.execution.clone(),
            ..self.allocation.work().clone()
        }
    }
    /// Original lexical checkout name.
    pub fn root_path(&self) -> &Path {
        &self.root
    }
    /// Original lexical operation-directory name.
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
    pub(super) fn parent<'a>(
        &self,
        entry: &(&HostRoot, &'a Path),
    ) -> Result<(cap_std::fs::Dir, &'a Path), NativeTreeMaterializationError> {
        self.work().charge(crate::WorkCounters {
            source_path_components: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let parent = entry.0.open_dir_held(
            entry.1.parent().unwrap_or_else(|| Path::new("")),
            &mut &self.work(),
        )?;
        Ok((
            parent,
            Path::new(
                entry
                    .1
                    .file_name()
                    .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
            ),
        ))
    }
    pub(super) fn stat(
        &self,
        entry: &(&HostRoot, &Path),
    ) -> Result<crate::native_host::HostStat, NativeTreeMaterializationError> {
        let (parent, leaf) = self.parent(entry)?;
        native_stat_held(&parent, leaf, &self.work())
    }
    pub(super) fn exists(
        &self,
        entry: &(&HostRoot, &Path),
    ) -> Result<bool, NativeTreeMaterializationError> {
        match self.stat(entry) {
            Ok(_) => Ok(true),
            Err(NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }
    pub(super) fn fingerprint(
        &self,
        entry: &(&HostRoot, &Path),
        contents: bool,
        desired: Option<&NativeMetadataImage>,
    ) -> Result<Option<[u8; 32]>, NativeTreeMaterializationError> {
        let (parent, leaf) = match self.parent(entry) {
            Ok(value) => value,
            Err(NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let metadata = match native_stat_held(&parent, leaf, &self.work()) {
            Ok(value) => value,
            Err(NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        let mut hasher = blake3::Hasher::new();
        if contents {
            hash_native_entry(self, &parent, leaf, &metadata, &mut hasher)?;
        } else {
            hash_native_metadata_edit(&metadata, desired, &mut hasher, &self.work())?;
        }
        Ok(Some(*hasher.finalize().as_bytes()))
    }
    fn create_parents(
        &self,
        entry: &(&HostRoot, &Path),
    ) -> Result<(), NativeTreeMaterializationError> {
        let directory = entry.0.open_dir_held(Path::new(""), &mut &self.work())?;
        crate::native_host::create_directories_held(
            &directory,
            entry.1.parent().unwrap_or_else(|| Path::new("")),
            &mut &self.work(),
        )?;
        Ok(())
    }
    fn rename(
        &self,
        from: &(&HostRoot, &Path),
        to: &(&HostRoot, &Path),
    ) -> Result<(), NativeTreeMaterializationError> {
        let (source, name) = self.parent(from)?;
        let (destination, target) = self.parent(to)?;
        self.work().admit(crate::WorkCounters {
            materializations: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        crate::native_host::rename_no_replace_held(
            &source,
            name,
            &destination,
            target,
            &mut &self.work(),
        )?;
        self.work().spent(crate::WorkCounters {
            materializations: 1,
            ..crate::WorkCounters::UNCHARGED
        })?;
        let a = crate::native_host::sync_directory_held(&source, &mut &self.work());
        let b = crate::native_host::sync_directory_held(&destination, &mut &self.work());
        a.and(b)?;
        Ok(())
    }
    fn remove(&self, entry: &(&HostRoot, &Path)) -> Result<(), NativeTreeMaterializationError> {
        let (parent, leaf) = match self.parent(entry) {
            Ok(value) => value,
            Err(NativeTreeMaterializationError::Io(error))
                if error.kind() == std::io::ErrorKind::NotFound =>
            {
                return Ok(());
            }
            Err(error) => return Err(error),
        };
        remove_native_held(&parent, leaf, &self.work())?;
        crate::native_host::sync_directory_held(&parent, &mut &self.work())?;
        Ok(())
    }
}

impl MaterializationBackend for std::sync::Arc<NativeTreeMaterializationBackend> {
    type Error = NativeTreeMaterializationError;

    fn native_work<T>(
        &self,
        operation: impl FnOnce(&NativeMaterializationWork) -> Result<T, NativeTreeMaterializationError>,
    ) -> Result<Option<T>, Self::Error> {
        operation(&self.work()).map(Some)
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
        Ok(Some(self.work().root_identity(&self.root)?.to_bytes()))
    }
    async fn observe_preparation_identity(
        &self,
    ) -> Result<Option<MaterializationPreparationIdentity>, Self::Error> {
        let work = self.work();
        let operation = HostRoot::open(&self.operation, &mut &work)?;
        let target = operation.open_dir_held(Path::new("target"), &mut &work)?;
        let backup = operation.open_dir_held(Path::new("backup"), &mut &work)?;
        Ok(Some(MaterializationPreparationIdentity {
            operation: operation.identity().to_bytes(),
            target: crate::native_host::directory_identity(&target, &mut &work)?.to_bytes(),
            backup: crate::native_host::directory_identity(&backup, &mut &work)?.to_bytes(),
        }))
    }
    async fn settle_observed(&self, edit: &MaterializationEdit) -> Result<(), Self::Error> {
        let input = duplicate_native_edit(edit, &self.work())?;
        let backend = self.clone();
        acyclic_native_runtime::run_blocking_io(
            move || {
                let input = input;
                let _allocation = input.1;
                let edit = input.0;
                let path = Path::new(edit_path(&edit));
                for root in [
                    &backend.held_root,
                    &backend.held_target,
                    &backend.held_backup,
                ] {
                    match root.open_dir_held(
                        path.parent().unwrap_or_else(|| Path::new("")),
                        &mut &backend.work(),
                    ) {
                        Ok(parent) => {
                            crate::native_host::sync_directory_held(&parent, &mut &backend.work())?
                        }
                        Err(NativeTreeMaterializationError::Io(error))
                            if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => return Err(error),
                    }
                }
                Ok(())
            },
            Some(self.work().runtime_meter()),
        )
        .map_err(NativeTreeMaterializationError::from_runtime_error)?
        .await?
    }

    async fn capture(
        &self,
        edit: &MaterializationEdit,
    ) -> Result<MaterializationPreimage, Self::Error> {
        let backend = self.clone();
        let input = duplicate_native_edit(edit, &self.work())?;
        acyclic_native_runtime::run_blocking_io(move || {
            let input = input;
            let _allocation = input.1;
            let edit = input.0;
            let edit = &edit;
            let path = edit_path(edit);
            let (live, target, backup) = backend.paths(path);
            if backend.exists(&backup)? {
                return Err(NativeTreeMaterializationError::UnexpectedBackup);
            }
            let before = backend.fingerprint(&live, !matches!(edit, MaterializationEdit::SetMetadata { .. }), None)?;
            if matches!(edit, MaterializationEdit::Install { image, .. } if image.starts_with(NATIVE_PREPARATION_ABSENT_IMAGE_PREFIX)) && before.is_some() {
                return Err(NativeTreeMaterializationError::ExternalMutation);
            }
            let after = match edit {
                MaterializationEdit::Install { .. } => backend.fingerprint(&target, true, None)?
                    .ok_or_else(|| NativeTreeMaterializationError::MissingEntry)?
                    .into(),
                MaterializationEdit::Remove { .. } => None,
                MaterializationEdit::SetMetadata { image, .. } => {
                    let desired = decode_native_metadata(image)?;
                    backend.fingerprint(&live, false, Some(&desired))?
                }
                MaterializationEdit::Rename { .. } => {
                    return Err(NativeTreeMaterializationError::UnsupportedRename);
                }
            };
            let metadata = if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                Some(native_metadata(&backend.stat(&live)?))
            } else {
                None
            };
            encode_native_witness(before, after, metadata, &backend.work())
        }, Some(self.work().runtime_meter())).map_err(NativeTreeMaterializationError::from_runtime_error)?
        .await?
    }

    async fn observe(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<MaterializationObservation, Self::Error> {
        let backend = self.clone();
        let input = duplicate_native_edit(edit, &self.work())?;
        let (image, allocation) = self.work().copy_bytes(&preimage.image)?;
        let preimage = MaterializationPreimage {
            image,
            custody: Some(allocation),
        };
        acyclic_native_runtime::run_blocking_io(
            move || {
                let input = input;
                let _allocation = input.1;
                let edit = input.0;
                let edit = &edit;
                let preimage = &preimage;
                let (before, after) = decode_native_witness(&preimage.image)?;
                let path = edit_path(edit);
                let (live, target, backup) = backend.paths(path);
                let current = backend.fingerprint(
                    &live,
                    !matches!(edit, MaterializationEdit::SetMetadata { .. }),
                    None,
                )?;
                let target_current = if matches!(edit, MaterializationEdit::Install { .. }) {
                    backend.fingerprint(&target, true, None)?
                } else {
                    None
                };
                let backup_current = backend.fingerprint(&backup, true, None)?;
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
            Some(self.work().runtime_meter()),
        )
        .map_err(NativeTreeMaterializationError::from_runtime_error)?
        .await?
    }

    async fn apply(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let backend = self.clone();
        let input = duplicate_native_edit(edit, &self.work())?;
        let (image, allocation) = self.work().copy_bytes(&preimage.image)?;
        let preimage = MaterializationPreimage {
            image,
            custody: Some(allocation),
        };
        acyclic_native_runtime::run_blocking_io(
            move || {
                let input = input;
                let _allocation = input.1;
                let edit = input.0;
                let edit = &edit;
                let preimage = &preimage;
                let path = edit_path(edit);
                let (live, target, backup) = backend.paths(path);
                let (before, after) = decode_native_witness(&preimage.image)?;
                if let MaterializationEdit::SetMetadata { image, .. } = edit {
                    let current = backend.fingerprint(
                        &live,
                        !matches!(edit, MaterializationEdit::SetMetadata { .. }),
                        None,
                    )?;
                    if current == after {
                        return Ok(());
                    }
                    if current != before {
                        return Err(NativeTreeMaterializationError::ExternalMutation);
                    }
                    return backend.set_metadata(&live, &decode_native_metadata(image)?, before);
                }
                let target_expected = matches!(edit, MaterializationEdit::Install { .. });
                let backup_exists = backend.exists(&backup)?;
                let target_fingerprint = backend.fingerprint(&target, true, None)?;
                let target_exists = target_fingerprint.is_some();
                let live_fingerprint = backend.fingerprint(&live, true, None)?;
                let live_exists = live_fingerprint.is_some();
                if !target_exists && live_fingerprint == after {
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
                        backend.remove(&live)?;
                        backend.rename(&target, &live)?;
                    } else if target_expected && !live_exists {
                        return Err(NativeTreeMaterializationError::MissingEntry);
                    } else if !target_expected {
                        backend.remove(&live)?;
                    }
                    return Ok(());
                }
                if live_exists {
                    backend.create_parents(&backup)?;
                    backend.rename(&live, &backup)?;
                }
                if target_expected {
                    if !target_exists {
                        return Err(NativeTreeMaterializationError::MissingEntry);
                    }
                    backend.create_parents(&live)?;
                    if let Err(error) = backend.rename(&target, &live) {
                        if backend.exists(&backup)? && !backend.exists(&live)? {
                            backend.rename(&backup, &live)?;
                        }
                        return Err(error);
                    }
                }
                Ok(())
            },
            Some(self.work().runtime_meter()),
        )
        .map_err(NativeTreeMaterializationError::from_runtime_error)?
        .await?
    }

    async fn restore(
        &self,
        edit: &MaterializationEdit,
        preimage: &MaterializationPreimage,
    ) -> Result<(), Self::Error> {
        let backend = self.clone();
        let input = duplicate_native_edit(edit, &self.work())?;
        let (image, allocation) = self.work().copy_bytes(&preimage.image)?;
        let preimage = MaterializationPreimage {
            image,
            custody: Some(allocation),
        };
        acyclic_native_runtime::run_blocking_io(
            move || {
                let input = input;
                let _allocation = input.1;
                let edit = input.0;
                let edit = &edit;
                let preimage = &preimage;
                let path = edit_path(edit);
                let (live, _, backup) = backend.paths(path);
                let (before, after) = decode_native_witness(&preimage.image)?;
                let current = backend.fingerprint(
                    &live,
                    !matches!(edit, MaterializationEdit::SetMetadata { .. }),
                    None,
                )?;
                if matches!(edit, MaterializationEdit::SetMetadata { .. }) {
                    if current == before {
                        return Ok(());
                    }
                    if current != after {
                        return Err(NativeTreeMaterializationError::ExternalMutation);
                    }
                    let metadata = decode_native_metadata(
                        preimage
                            .image
                            .get(67..)
                            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
                    )?;
                    return backend.set_metadata(&live, &metadata, after);
                }
                let backup_current = backend.fingerprint(&backup, true, None)?;
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
                    None => backend.remove(&live),
                    Some(_) if backup_exists => {
                        backend.remove(&live)?;
                        backend.rename(&backup, &live)
                    }
                    Some(expected) if current == Some(expected) => Ok(()),
                    Some(_) => Err(NativeTreeMaterializationError::MissingEntry),
                }
            },
            Some(self.work().runtime_meter()),
        )
        .map_err(NativeTreeMaterializationError::from_runtime_error)?
        .await?
    }
}
