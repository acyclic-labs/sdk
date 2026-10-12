use super::*;
use crate::native_host::HostRoot;
use std::sync::Arc;

impl NativeTreeMaterializationBackend {
    pub(super) fn set_metadata(
        &self,
        entry: &(&HostRoot, &Path),
        desired: &NativeMetadataImage,
        expected: Option<[u8; 32]>,
    ) -> Result<(), NativeTreeMaterializationError> {
        let (parent, leaf) = self.parent(entry)?;
        let metadata = native_stat_held(&parent, leaf, &self.work())?;
        let file = with_native_walk_leaf(leaf, &self.work(), |name| {
            if metadata.is_dir() {
                Ok(open_native_walk_directory(&parent, name, &self.work())?.into_std_file())
            } else {
                crate::native_host::open_leaf(
                    &parent,
                    leaf,
                    &crate::native_host::NativeOpenOptions::Read,
                    &mut &self.work(),
                )
            }
        })?;
        let actual = crate::native_host::stat_file(&file, &mut &self.work())?;
        let mut hasher = blake3::Hasher::new();
        hash_native_metadata_edit(&actual, None, &mut hasher, &self.work())?;
        if actual
            .identity()
            .map_err(native_materialization_work_error)?
            != metadata
                .identity()
                .map_err(native_materialization_work_error)?
            || Some(*hasher.finalize().as_bytes()) != expected
        {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        #[cfg(windows)]
        {
            let attributes = desired.windows_attributes.unwrap_or_else(|| {
                if desired.readonly {
                    actual.file_attributes() | 1
                } else {
                    actual.file_attributes() & !1
                }
            });
            crate::native_host::set_file_attributes_held(
                &parent,
                leaf,
                attributes,
                Some(
                    actual
                        .identity()
                        .map_err(native_materialization_work_error)?,
                ),
                &self.work(),
            )?;
        }
        #[cfg(unix)]
        {
            use std::os::fd::AsRawFd as _;
            if let Some(mode) = desired.posix_mode {
                loop {
                    self.work()
                        .charge(NativeResourceCredit::WriteOperations.delta(1))?;
                    // SAFETY: file owns the descriptor for this witnessed allocation.
                    if unsafe { libc::fchmod(file.as_raw_fd(), mode as libc::mode_t) } == 0 {
                        break;
                    }
                    let error = std::io::Error::last_os_error();
                    if error.kind() != std::io::ErrorKind::Interrupted {
                        return Err(error.into());
                    }
                }
                crate::native_host::sync_file_held_with_work(&file, &mut &self.work())?;
            }
        }
        Ok(())
    }
    /// Observes a mapping through the original held root.
    pub fn mapping_root_identity(
        &self,
        path: &str,
    ) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        self.check_path(path)?;
        let actual = self.work().root_identity(self.root_path())?;
        let expected = self.held_root.identity();
        if actual != expected {
            return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual });
        }
        let directory = self
            .held_root
            .open_dir_held(Path::new(path), &mut &self.work())?;
        crate::native_host::directory_identity(&directory, &mut &self.work())
    }
    /// Admitted lexical staging path.
    pub fn staging_path(
        &self,
        path: &str,
    ) -> Result<(PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        self.check_path(path)?;
        let (mut target, mut allocation) = self
            .work()
            .path(self.operation_directory_path(), Some(Path::new("target")))?;
        if !path.is_empty() {
            let work = self.work();
            let delta = NativeResourceCredit::Copy.delta(path.len() as u64);
            work.admit(delta)?;
            let needed = target
                .as_os_str()
                .len()
                .checked_add(path.len())
                .and_then(|bytes| bytes.checked_add(1))
                .ok_or(crate::WorkError::Overflow)?;
            if needed > target.capacity() {
                allocation.grow((needed - target.capacity()) as u64)?;
                let reserved = target.try_reserve_exact(needed - target.as_os_str().len());
                let reconciled = allocation.reconcile_capacity(target.capacity() as u64);
                reserved.map_err(NativeTreeMaterializationError::AllocationFailed)?;
                reconciled?;
            }
            target.push(path);
            work.spent(delta)?;
        }
        Ok((target, allocation))
    }
    fn check_path(&self, path: &str) -> Result<(), NativeTreeMaterializationError> {
        if !path.is_empty()
            && !crate::path::is_canonical_relative_with_step(path, || {
                self.work().charge(NativeResourceCredit::Items.delta(1))
            })?
        {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(())
    }
    /// Requires all three original allocations to be empty.
    pub fn require_initial_empty(&self) -> Result<(), NativeTreeMaterializationError> {
        for root in [&self.held_root, &self.held_target, &self.held_backup] {
            let directory = root.open_dir_held(Path::new(""), &mut &self.work())?;
            let (mut reader, _cursor) =
                crate::native_host::read_dir_held(&directory, &mut &self.work())?;
            if next_native_walk_name(&mut reader, &self.work())?.is_some() {
                return Err(NativeTreeMaterializationError::ExternalMutation);
            }
        }
        Ok(())
    }
    /// Allows only selected root bindings and explicitly preserved names.
    pub fn require_selected_root(
        &self,
        edits: &[MaterializationEdit],
        preserved: &[&str],
    ) -> Result<(), NativeTreeMaterializationError> {
        for path in preserved {
            self.check_path(path)?;
        }
        let directory = self
            .held_root
            .open_dir_held(Path::new(""), &mut &self.work())?;
        let (mut reader, _cursor) =
            crate::native_host::read_dir_held(&directory, &mut &self.work())?;
        while let Some(entry) = next_native_walk_name(&mut reader, &self.work())? {
            let _allocation = entry.1;
            let name = entry.0;
            let mut selected = false;
            for path in edits
                .iter()
                .filter_map(|edit| match edit {
                    MaterializationEdit::Install { path, .. } => Some(path.as_str()),
                    _ => None,
                })
                .chain(preserved.iter().copied())
            {
                let top = path.split('/').next().unwrap_or_default();
                #[cfg(unix)]
                let matches = {
                    use std::os::unix::ffi::OsStrExt as _;
                    native_compare(name.as_bytes().iter().copied(), top.bytes(), &self.work())?
                        == std::cmp::Ordering::Equal
                };
                #[cfg(windows)]
                let matches =
                    native_compare(name.iter().copied(), top.encode_utf16(), &self.work())?
                        == std::cmp::Ordering::Equal;
                if matches {
                    selected = true;
                    break;
                }
            }
            if !selected {
                return Err(NativeTreeMaterializationError::ExternalMutation);
            }
        }
        Ok(())
    }
    /// Clears only the witnessed staging allocation, preserving its root identity.
    pub async fn reset_target_directory(
        self: &Arc<Self>,
        path: &str,
    ) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        self.check_path(path)?;
        if self.observe_allocation_identity().await? != self.allocation_identity()
            || self.observe_preparation_identity().await? != self.preparation_identity()
        {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        let input = self.work().copy_bytes(path.as_bytes())?;
        let backend = self.clone();
        acyclic_native_runtime::run_blocking_io(
            move || {
                let input = input;
                let _allocation = input.1;
                let path = String::from_utf8(input.0)
                    .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
                let directory = backend
                    .held_target
                    .open_dir_held(Path::new(""), &mut &backend.work())?;
                let identity = if path.is_empty() {
                    let (mut reader, _cursor) =
                        crate::native_host::read_dir_held(&directory, &mut &backend.work())?;
                    while let Some(entry) = next_native_walk_name(&mut reader, &backend.work())? {
                        let _allocation = entry.1;
                        let name = entry.0;
                        remove_native_walk(&directory, &name, &backend.work())?;
                    }
                    backend.held_target.identity()
                } else {
                    let path = Path::new(&path);
                    let parent = backend
                        .held_target
                        .open_dir_held(Path::new(""), &mut &backend.work())?;
                    crate::native_host::create_directories_held(
                        &parent,
                        path.parent().unwrap_or_else(|| Path::new("")),
                        &mut &backend.work(),
                    )?;
                    let parent = backend.held_target.open_dir_held(
                        path.parent().unwrap_or_else(|| Path::new("")),
                        &mut &backend.work(),
                    )?;
                    let leaf = Path::new(
                        path.file_name()
                            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
                    );
                    remove_native_held(&parent, leaf, &backend.work())?;
                    let directory = crate::native_host::create_new_directory_held(
                        &parent,
                        leaf,
                        &mut &backend.work(),
                    )?;
                    crate::native_host::directory_identity(&directory, &mut &backend.work())?
                };
                crate::native_host::sync_directory_held(&directory, &mut &backend.work())?;
                Ok(identity)
            },
            Some((self.work().runtime_meter(), false)),
        )
        .map_err(NativeTreeMaterializationError::from_runtime_error)?
        .await?
    }
}
