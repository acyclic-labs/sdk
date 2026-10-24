use super::*;

#[cfg(windows)]
pub(super) fn native_utf16_name_bytes(
    units: impl Iterator<Item = u16>,
) -> impl Iterator<Item = u8> {
    std::char::decode_utf16(units).flat_map(|unit| {
        let mut bytes = [0; 4];
        let length = match unit {
            Ok(character) => character.encode_utf8(&mut bytes).len(),
            Err(error) => {
                let value = error.unpaired_surrogate();
                bytes[..3].copy_from_slice(&[
                    0xe0 | (value >> 12) as u8,
                    0x80 | ((value >> 6) & 63) as u8,
                    0x80 | (value & 63) as u8,
                ]);
                3
            }
        };
        bytes.into_iter().take(length)
    })
}

#[cfg(all(any(test, feature = "local"), feature = "native-mount"))]
pub(super) fn native_compare<T: Ord>(
    mut a: impl Iterator<Item = T>,
    mut b: impl Iterator<Item = T>,
    work: &NativeMaterializationWork,
) -> Result<std::cmp::Ordering, NativeTreeMaterializationError> {
    loop {
        work.charge(NativeResourceCredit::Items.delta(1))?;
        match (a.next(), b.next()) {
            (Some(a), Some(b)) if a == b => {}
            (a, b) => return Ok(a.cmp(&b)),
        }
    }
}

impl NativeTreeMaterializationBackend {
    fn check_path(&self, path: &str) -> Result<(), NativeTreeMaterializationError> {
        if path.is_empty()
            || crate::path::is_canonical_relative_with_step(path, || {
                self.work.charge(NativeResourceCredit::Items.delta(1))
            })?
        {
            Ok(())
        } else {
            Err(NativeTreeMaterializationError::InvalidLayout)
        }
    }
    /// Observes a binding through its original checkout allocation.
    pub fn mapping_root_identity(
        &self,
        path: &str,
    ) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        self.check_path(path)?;
        let actual = self.work.root_identity(self.root_path())?;
        let expected = self.held_root.identity();
        if actual != expected {
            return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual });
        }
        let directory = host_path(Path::new(path), &mut &self.work, |path, work| {
            self.held_root.open_dir_held(path, work)
        })?;
        crate::native_host::directory_identity(&directory, &mut &self.work)
    }
    /// Returns an admitted staging path.
    pub fn staging_path(
        &self,
        path: &str,
    ) -> Result<(PathBuf, NativeAllocation), NativeTreeMaterializationError> {
        self.check_path(path)?;
        let (base, mut allocation) = self
            .work
            .path(self.operation_directory_path(), Some(Path::new("target")))?;
        let (result, mut extra) = self.work.path(&base, Some(Path::new(path)))?;
        allocation.absorb(&mut extra)?;
        Ok((result, allocation))
    }
    /// Requires empty recovery allocations.
    pub fn require_empty_staging(&self) -> Result<(), NativeTreeMaterializationError> {
        for root in [&self.held_target, &self.held_backup] {
            let (mut reader, _cursor) = host_path(Path::new(""), &mut &self.work, |path, work| {
                root.read_dir_stats(path, work)
            })?;
            if reader.next(&mut &self.work)?.is_some() {
                return Err(NativeTreeMaterializationError::ExternalMutation);
            }
        }
        Ok(())
    }
    /// Rejects root entries outside the selected bindings and preserved names.
    pub fn require_selected_root(
        &self,
        edits: &[MaterializationEdit],
        preserved: &[&str],
        mut owned: impl FnMut(
            &crate::native_host::HostListedEntry,
        ) -> Result<bool, NativeTreeMaterializationError>,
    ) -> Result<(), NativeTreeMaterializationError> {
        for path in preserved {
            self.check_path(path)?;
        }
        let (mut reader, _cursor) = host_path(Path::new(""), &mut &self.work, |path, work| {
            self.held_root.read_dir_stats(path, work)
        })?;
        while let Some((entry, _allocation)) = reader.next(&mut &self.work)? {
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
                self.work
                    .charge(NativeResourceCredit::Items.delta(top.len() as u64))?;
                if entry.is_named(top) {
                    selected = true;
                    break;
                }
            }
            if !selected && !owned(&entry)? {
                return Err(NativeTreeMaterializationError::ExternalMutation);
            }
        }
        Ok(())
    }
    /// Clears staged contents while retaining the witnessed target root.
    pub async fn reset_target_directory(
        self: &std::sync::Arc<Self>,
        path: &str,
    ) -> Result<crate::NativeRootIdentity, NativeTreeMaterializationError> {
        self.check_path(path)?;
        if self.observe_allocation_identity().await? != self.allocation_identity()
            || self.observe_preparation_identity().await? != self.preparation_identity()
        {
            return Err(NativeTreeMaterializationError::ExternalMutation);
        }
        let (path, allocation) = self.work.copy_bytes(path.as_bytes())?;
        let backend = self.clone();
        acyclic_native_runtime::run_blocking_io(
            move || {
                let _allocation = allocation;
                let path = std::str::from_utf8(&path)
                    .map_err(|_| NativeTreeMaterializationError::InvalidLayout)?;
                let path = Path::new(path);
                if !path.as_os_str().is_empty()
                    && host_stat(&backend.held_target, path, &backend.work)?
                        .is_some_and(|stat| !stat.is_dir())
                {
                    remove_native_entry_durable(&(&backend.held_target, path), &backend.work)?;
                }
                let directory = backend
                    .work
                    .with_original_leaf(|state| {
                        host_path(path, state, |path, work| {
                            backend.held_target.create_dir_all_held(path, work)
                        })
                    })
                    .map_err(native_materialization_work_error)?;
                let (mut reader, _cursor) =
                    crate::native_host::read_dir_held(&directory.directory, &mut &backend.work)?;
                while let Some((entry, _allocation)) = reader.next(&mut &backend.work)? {
                    backend
                        .work
                        .with_original_leaf(|state| {
                            directory.remove(
                                #[cfg(unix)]
                                Path::new(&entry.name),
                                #[cfg(windows)]
                                &entry.name,
                                None,
                                state,
                            )
                        })
                        .map_err(native_materialization_work_error)?;
                    crate::native_host::sync_directory_held(
                        &directory.directory,
                        &mut &backend.work,
                    )?;
                }
                if let Some(parent) = path.parent() {
                    let parent = held_child(&backend.held_target, parent, &backend.work)?;
                    crate::native_host::sync_directory_held(&parent.directory, &mut &backend.work)?;
                }
                Ok(directory.identity())
            },
            Some((self.work.runtime_meter(), true)),
        )
        .map_err(NativeTreeMaterializationError::from_runtime_error)?
        .await?
    }
}
