use super::*;
use crate::native_host::HostStatReader;

pub(super) fn native_utf16_name_bytes(
    units: impl Iterator<Item = u16>,
) -> impl Iterator<Item = u8> {
    std::char::decode_utf16(units).flat_map(|unit| {
        let value = unit
            .map(u32::from)
            .unwrap_or_else(|error| u32::from(error.unpaired_surrogate()));
        let mut bytes = [0; 4];
        let length = if value < 0x80 {
            bytes[0] = value as u8;
            1
        } else if value < 0x800 {
            bytes[0] = 0xc0 | (value >> 6) as u8;
            bytes[1] = 0x80 | (value & 63) as u8;
            2
        } else if value < 0x10000 {
            bytes[0] = 0xe0 | (value >> 12) as u8;
            bytes[1] = 0x80 | ((value >> 6) & 63) as u8;
            bytes[2] = 0x80 | (value & 63) as u8;
            3
        } else {
            bytes[0] = 0xf0 | (value >> 18) as u8;
            bytes[1] = 0x80 | ((value >> 12) & 63) as u8;
            bytes[2] = 0x80 | ((value >> 6) & 63) as u8;
            bytes[3] = 0x80 | (value & 63) as u8;
            4
        };
        bytes.into_iter().take(length)
    })
}

pub(super) fn next_name(
    reader: &mut HostStatReader,
    work: &NativeMaterializationWork,
) -> Result<Option<(std::ffi::OsString, NativeAllocation)>, NativeTreeMaterializationError> {
    #[cfg(unix)]
    return reader
        .next(&mut &*work)
        .map(|entry| entry.map(|(entry, allocation)| (entry.name, allocation)));
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStringExt as _;
        let Some((entry, allocation)) = reader.next(&mut &*work)? else {
            return Ok(None);
        };
        let mut owned = work.claim(
            owned_size(entry.name.len(), 3)?,
            1,
            crate::WorkCounters::UNCHARGED,
        )?;
        let name = std::ffi::OsString::from_wide(&entry.name);
        owned.reconcile_capacity(name.capacity() as u64)?;
        drop(entry);
        drop(allocation);
        Ok(Some((name, owned)))
    }
}

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
        let actual = self.work.root_identity(&self.root_path())?;
        let expected = self.held_root.identity();
        if actual != expected {
            return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual });
        }
        let directory = self
            .held_root
            .open_dir_held(Path::new(path), &mut &self.work)?;
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
    /// Requires empty original checkout and recovery allocations.
    pub fn require_initial_empty(&self) -> Result<(), NativeTreeMaterializationError> {
        for root in [&self.held_root, &self.held_target, &self.held_backup] {
            let (mut reader, _cursor) = root.read_dir_stats(Path::new(""), &mut &self.work)?;
            if next_name(&mut reader, &self.work)?.is_some() {
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
    ) -> Result<(), NativeTreeMaterializationError> {
        for path in preserved {
            self.check_path(path)?;
        }
        let (mut reader, _cursor) = self
            .held_root
            .read_dir_stats(Path::new(""), &mut &self.work)?;
        while let Some((name, _allocation)) = next_name(&mut reader, &self.work)? {
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
                if name == std::ffi::OsStr::new(top) {
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
                if path.is_empty() {
                    let (mut reader, _cursor) = backend
                        .held_target
                        .read_dir_stats(Path::new(""), &mut &backend.work)?;
                    while let Some((name, _allocation)) = next_name(&mut reader, &backend.work)? {
                        remove_native_entry_durable(
                            &(&backend.held_target, Path::new(&name)),
                            &backend.work,
                        )?;
                    }
                    return Ok(backend.held_target.identity());
                }
                let path = Path::new(path);
                let parent = backend
                    .work
                    .with_original_leaf(|state| {
                        backend.held_target.create_dir_all_held(
                            path.parent().ok_or(crate::DemandError::InvalidRequest)?,
                            state,
                        )
                    })
                    .map_err(native_materialization_work_error)?;
                if host_stat(&backend.held_target, path, &backend.work)?.is_some() {
                    remove_native_entry_durable(&(&backend.held_target, path), &backend.work)?;
                }
                let leaf = Path::new(
                    path.file_name()
                        .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
                );
                backend
                    .work
                    .with_original_leaf(|state| parent.create_dir(leaf, state))
                    .map_err(native_materialization_work_error)?;
                crate::native_host::sync_directory_held(&parent.directory, &mut &backend.work)?;
                let directory = parent.open_dir_held(leaf, &mut &backend.work)?;
                crate::native_host::directory_identity(&directory, &mut &backend.work)
            },
            Some((self.work.runtime_meter(), true)),
        )
        .map_err(NativeTreeMaterializationError::from_runtime_error)?
        .await?
    }
}
