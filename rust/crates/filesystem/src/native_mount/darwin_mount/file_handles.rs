use super::*;

impl DarwinMountContext {
    pub(super) fn open(
        &self,
        path: &MountPath,
        written: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        let stamp = self.cache_stamp(work)?;
        let lookup = self.lookup(path, work)?;
        match lookup.node.kind {
            MountNodeKind::Regular => {}
            MountNodeKind::Directory => return Err(driver_errno(libc::EISDIR)),
            _ => return Err(driver_errno(libc::EINVAL)),
        }
        let handle = self.allocate_handle().map_err(driver_errno)?;
        let observation = shared(work, Mutex::new(FileObservation { stamp, lookup }))?;
        let written = shared(work, AtomicU64::new(written))?;
        let mut files = self.files.write().map_err(|_| driver_errno(libc::EIO))?;
        grow_map(&mut files, work)?;
        copying(
            work,
            copy_work(std::mem::size_of::<(u64, FileHandle)>() as u64),
            || {
                files.insert(
                    handle,
                    FileHandle {
                        file_id: lookup.node.file_id,
                        file: None,
                        observation,
                        written,
                    },
                )
            },
        )?;
        Ok(handle)
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
        let (file_id, written) = {
            let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;
            work.charge(scan_work(files.capacity() as u64))?;
            let entry = files
                .get(&handle)
                .ok_or_else(|| driver_errno(libc::ESTALE))?;
            let written = Some(Arc::clone(&entry.written));
            if let Some((file, custody)) = &entry.file {
                return Ok(FileTarget {
                    file: (Arc::clone(file), custody.clone()),
                    written,
                });
            }
            (entry.file_id, written)
        };
        let (file, lookup) = self.source.open_file_with_lookup_with_meter(path, work)?;
        let file = retained_file(work, file)?;
        if lookup.node.file_id != file_id {
            return Err(driver_errno(libc::ESTALE));
        }
        let mut files = self.files.write().map_err(|_| driver_errno(libc::EIO))?;
        work.charge(scan_work(files.capacity() as u64))?;
        let entry = files
            .get_mut(&handle)
            .ok_or_else(|| driver_errno(libc::ESTALE))?;
        let (file, custody) = entry.file.get_or_insert(file);
        Ok(FileTarget {
            file: (Arc::clone(file), custody.clone()),
            written,
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
            let mut files = self.files.write().map_err(|_| driver_errno(libc::EIO))?;
            work.charge(scan_work(files.capacity() as u64))?;
            for entry in files
                .values_mut()
                .filter(|entry| entry.file_id == lookup.node.file_id)
            {
                entry.file = Some((Arc::clone(&detached.0), detached.1.clone()));
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
            written.fetch_max(sequence, Ordering::AcqRel);
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
        let observation = {
            let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;
            work.charge(scan_work(files.capacity() as u64))?;
            Arc::clone(
                &files
                    .get(&handle)
                    .ok_or_else(|| driver_errno(libc::ESTALE))?
                    .observation,
            )
        };
        {
            let observation = observation.lock().map_err(|_| driver_errno(libc::EIO))?;
            if let Some(stamp) = observation.stamp
                && self.unchanged_since(path, Some(observation.lookup.node.file_id), stamp, work)?
            {
                return Ok(observation.lookup);
            }
        }
        let stamp = self.cache_stamp(work)?;
        let lookup = self
            .file_target(path, handle, work)?
            .file
            .0
            .lookup_with_meter(work)?;
        if stamp.is_some() {
            let mut observation = observation.lock().map_err(|_| driver_errno(libc::EIO))?;
            observation.stamp = stamp;
            observation.lookup = lookup;
        }
        Ok(lookup)
    }
}
