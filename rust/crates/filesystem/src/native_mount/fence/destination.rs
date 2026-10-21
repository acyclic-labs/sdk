use super::*;

#[cfg(test)]
#[cfg(all(target_os = "linux", not(acyclic_fs_native_loom)))]
mod retry_tests;
#[cfg(test)]
#[cfg(all(target_os = "linux", not(acyclic_fs_native_loom)))]
mod tests;

fn cleanup_work(_name: &[crate::native_host::NativeUnit]) -> crate::WorkCounters {
    crate::WorkCounters {
        backend_write_operations: 1,
        #[cfg(windows)]
        backend_read_operations: 1,
        #[cfg(windows)]
        items_examined: _name.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    }
}

impl MountDestinationGuard {
    pub(in crate::native_mount) fn prepare_destination_cleanup(
        &mut self,
        destination: &Path,
        native: &NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if self.destination_cleanup.is_some() {
            return Ok(());
        }
        if self.file.is_none() || self.parent_directory.is_none() {
            return Err(NativeMountError::ControlPending);
        }
        let leaf = destination
            .file_name()
            .ok_or(NativeMountError::InvalidDestination)?;
        let prepared = crate::native_host::prepare_leaf(Path::new(leaf), &mut &*native)?;
        let credit = native.reserve_work(cleanup_work(&prepared.0))?;
        self.destination_cleanup = Some((prepared.0, prepared.1, credit));
        Ok(())
    }

    pub(in crate::native_mount) fn remove_destination(&mut self) -> Result<(), NativeMountError> {
        let Some((name, _, credit)) = self.destination_cleanup.as_mut() else {
            return Ok(());
        };
        // Physical namespace producers have stopped, but this destination's
        // fence is still held. A new starter cannot acquire this destination.
        if self.file.is_none() {
            return Err(NativeMountError::ControlPending);
        }
        let parent = self
            .parent_directory
            .as_ref()
            .ok_or(NativeMountError::ControlPending)?;
        credit.refill_cleanup(cleanup_work(name))?;
        let mut work = NativeFenceReleaseWork {
            credit,
            wait: None,
            pending: None,
        };
        match crate::native_host::remove_prepared_leaf(parent, name, true, &mut work) {
            Ok(()) => {}
            Err(crate::NativeTreeMaterializationError::Io(error))
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
                ) => {}
            Err(error) => return Err(error.into()),
        }
        drop(self.destination_cleanup.take());
        Ok(())
    }
}
