use super::*;

impl DarwinMountContext {
    pub(super) fn open(
        &self,
        path: &MountPath,
        written: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        let lookup = self.lookup(path, work)?;
        match lookup.node.kind {
            MountNodeKind::Regular => {}
            MountNodeKind::Directory => return Err(driver_errno(libc::EISDIR)),
            _ => return Err(driver_errno(libc::EINVAL)),
        }
        let handle = self.allocate_handle().map_err(driver_errno)?;
        let entry = shared(
            work,
            DarwinOwned::empty(FileHandle {
                file_id: lookup.node.file_id,
                file: Mutex::new(None),
                written: AtomicU64::new(written),
            }),
        )?;
        let mut files = self.files.write().map_err(|_| driver_errno(libc::EIO))?;
        grow_map(&mut files, work)?;
        work.charge(copy_work(
            std::mem::size_of::<(u64, Arc<DarwinOwned<FileHandle>>)>() as u64,
        ))?;
        files.insert(handle, entry);
        Ok(handle)
    }

    fn handle(
        &self,
        handle: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Arc<DarwinOwned<FileHandle>>, NativeMountError> {
        let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;
        work.charge(scan_work(files.capacity() as u64))?;
        files
            .get(&handle)
            .cloned()
            .ok_or_else(|| driver_errno(libc::ESTALE))
    }

    pub(super) fn file_target(
        &self,
        path: &MountPath,
        handle: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<FileTarget, NativeMountError> {
        if handle == 0 {
            return Ok(FileTarget {
                file: retained_file(work, self.source.open_file_with_meter(path, work)?)?,
                written: None,
            });
        }
        let entry = self.handle(handle, work)?;
        let bound = entry
            .file
            .lock()
            .map_err(|_| driver_errno(libc::EIO))?
            .clone();
        let file = match bound {
            Some(file) => file,
            None => {
                let (file, lookup) = self.source.open_file_with_lookup_with_meter(path, work)?;
                let file = retained_file(work, file)?;
                if lookup.node.file_id != entry.file_id {
                    return Err(driver_errno(libc::ESTALE));
                }
                // Close may have removed the handle while the source was opening it.
                let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;
                work.charge(scan_work(files.capacity() as u64))?;
                if !files.contains_key(&handle) {
                    return Err(driver_errno(libc::ESTALE));
                }
                let file = entry
                    .file
                    .lock()
                    .map_err(|_| driver_errno(libc::EIO))?
                    .get_or_insert(file)
                    .clone();
                file
            }
        };
        Ok(FileTarget {
            file,
            written: Some(entry),
        })
    }

    pub(super) fn unlink(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        unlink: impl FnOnce() -> Result<(), MountSourceError>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let has_open = if lookup.node.kind == MountNodeKind::Regular && lookup.node.link_count == 1
        {
            let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;
            work.charge(scan_work(files.capacity() as u64))?;
            files
                .values()
                .any(|entry| entry.file_id == lookup.node.file_id)
        } else {
            false
        };
        let detached = self.mutate(
            || {
                let detached = if has_open {
                    Some(self.source.detach_file_with_meter(path, work)?)
                } else {
                    None
                };
                unlink()?;
                Ok(detached)
            },
            work,
        )?;
        if let Some(detached) = detached {
            let detached = retained_file(work, detached)?;
            let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;
            work.charge(scan_work(files.capacity() as u64))?;
            for entry in files
                .values()
                .filter(|entry| entry.file_id == lookup.node.file_id)
            {
                *entry.file.lock().map_err(|_| driver_errno(libc::EIO))? = Some(detached.clone());
            }
        }
        Ok(())
    }

    pub(super) fn mutate<T>(
        &self,
        mutation: impl FnOnce() -> Result<T, MountSourceError>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<T, NativeMountError> {
        self.admit_write(work)?;
        let result = mutation().map_err(NativeMountError::from);
        self.ledger.record();
        result.and_then(|value| {
            self.admit_write(work)?;
            Ok(value)
        })
    }

    pub(super) fn mutate_file<T>(
        &self,
        path: &MountPath,
        handle: u64,
        mutation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
        work: &crate::NativeMaterializationWork,
    ) -> Result<T, NativeMountError> {
        self.admit_write(work)?;
        let target = self.file_target(path, handle, work)?;
        let result = mutation(target.file.0.as_ref()).map_err(NativeMountError::from);
        let sequence = self.ledger.record();
        if let Some(written) = target.written {
            written.written.fetch_max(sequence, Ordering::AcqRel);
        }
        result.and_then(|value| {
            self.admit_write(work)?;
            Ok(value)
        })
    }

    pub(super) fn lookup_handle(
        &self,
        path: &MountPath,
        handle: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, NativeMountError> {
        if handle == 0 {
            return self.lookup(path, work);
        }
        let entry = self.handle(handle, work)?;
        let file = entry
            .file
            .lock()
            .map_err(|_| driver_errno(libc::EIO))?
            .clone();
        let lookup = match file {
            Some((file, _custody)) => file.lookup_with_meter(work)?,
            None => self.lookup(path, work)?,
        };
        if lookup.node.file_id != entry.file_id {
            return Err(driver_errno(libc::ESTALE));
        }
        Ok(lookup)
    }
}
