use super::*;

pub(super) fn contains_path_with_work<W: SourceDemandReadWork>(
    watches: &[(PathBuf, i32)],
    path: &Path,
    work: &mut W,
) -> Result<bool, DemandError> {
    for (candidate, _) in watches {
        work.with_original_leaf(|work| work.started_io(ITEM))?;
        if candidate == path {
            return Ok(true);
        }
    }
    Ok(false)
}
pub(super) fn prepare_insert_with_work<W: SourceDemandReadWork>(
    watches: &mut Vec<(PathBuf, i32)>,
    path: &Path,
    work: &mut W,
    retained: &AtomicU64,
    ownership: &SourcePayloadOwnership,
) -> Result<PathBuf, DemandError> {
    if watches.len() == watches.capacity() {
        let count = watches.len().checked_add(1).ok_or(WorkError::Overflow)?;
        let bytes = (count as u64)
            .checked_mul(std::mem::size_of::<(PathBuf, i32)>() as u64)
            .ok_or(WorkError::Overflow)?;
        let old_bytes = (watches.capacity() as u64)
            .checked_mul(std::mem::size_of::<(PathBuf, i32)>() as u64)
            .ok_or(WorkError::Overflow)?;
        let copied = copied(old_bytes);
        ownership.claim_retained(bytes, copied, work)?;
        let mut grown = Vec::with_capacity(count);
        grown.append(watches);
        *watches = grown;
        let grown_bytes = watches.capacity() as u64 * std::mem::size_of::<(PathBuf, i32)>() as u64;
        retained.fetch_add(grown_bytes - old_bytes, Ordering::AcqRel);
        ownership.retire(old_bytes)?;
        if !ownership.has_native_custody() {
            work.release(bytes)?;
        }
    }
    let bytes = path.as_os_str().len() as u64;
    let copied = copied(bytes);
    ownership.claim_retained(bytes, copied, work)?;
    Ok(path.to_path_buf())
}
pub(super) fn proc_fd_path(descriptor: i32) -> ([u8; 24], usize) {
    let mut path = [0_u8; 24];
    let prefix = b"/proc/self/fd/";
    let digits = rustix::path::DecInt::new(descriptor);
    for (target, byte) in path.iter_mut().zip(prefix.iter().chain(digits.as_bytes())) {
        *target = *byte;
    }
    (path, prefix.len() + digits.as_bytes().len())
}
#[allow(
    clippy::too_many_lines,
    reason = "Ancestor registration keeps its admitted allocations and rollback in one frame."
)]
pub(super) fn watch_ancestors<W: SourceDemandReadWork>(
    watch: &Shared,
    root: &OwnedFd,
    ancestors: &mut Vec<(i32, std::ffi::OsString)>,
    retained: &mut u64,
    cleanup: &mut StartupCleanup,
    work: &mut W,
) -> Result<(), DemandError> {
    use std::os::unix::ffi::OsStringExt;
    let (proc_path, _) = proc_fd_path(root.as_raw_fd());
    let mut path = Vec::new();
    let mut pending = std::ffi::OsString::new();
    let mut temporary = 0_u64;
    let result = (|| {
        let mut capacity = 256_usize;
        loop {
            let bytes = capacity as u64;
            let next = temporary.checked_add(bytes).ok_or(WorkError::Overflow)?;
            work.claim_allocation(bytes, 1, WorkCounters::UNCHARGED)?;
            temporary = next;
            let old = path.capacity() as u64;
            path = vec![0_u8; capacity];
            work.release(old)?;
            temporary -= old;
            work.admit(WorkCounters {
                source_bytes_read: bytes,
                ..WorkCounters::UNCHARGED
            })?;
            work.started_io(READ)?;
            // SAFETY: the C path and writable buffer live through readlink.
            let length = unsafe {
                libc::readlink(
                    proc_path.as_ptr().cast(),
                    path.as_mut_ptr().cast(),
                    path.len(),
                )
            };
            if length < 0 {
                return Err(io::Error::last_os_error().into());
            }
            let length = usize::try_from(length).map_err(|_| DemandError::InvalidRequest)?;
            work.record(WorkCounters {
                source_bytes_read: length as u64,
                ..WorkCounters::UNCHARGED
            })?;
            if length < capacity {
                path.truncate(length);
                break;
            }
            capacity = capacity.checked_mul(2).ok_or(WorkError::Overflow)?;
        }
        let mut child = Path::new(OsStr::from_bytes(&path));
        loop {
            work.started_io(ITEM)?;
            let (Some(parent), Some(name)) = (child.parent(), child.file_name()) else {
                break;
            };
            cleanup.extend(ITEM, work)?;
            let name_bytes = name.len() as u64;
            let next = temporary
                .checked_add(name_bytes)
                .ok_or(WorkError::Overflow)?;
            work.claim_allocation(name_bytes, u64::from(name_bytes != 0), copied(name_bytes))?;
            temporary = next;
            pending = std::ffi::OsString::from_vec(name.as_bytes().to_vec());
            if ancestors.len() == ancestors.capacity() {
                let count = ancestors.len().checked_add(1).ok_or(WorkError::Overflow)?;
                let item = std::mem::size_of::<(i32, std::ffi::OsString)>() as u64;
                let bytes = (count as u64)
                    .checked_mul(item)
                    .ok_or(WorkError::Overflow)?;
                let old = (ancestors.capacity() as u64)
                    .checked_mul(item)
                    .ok_or(WorkError::Overflow)?;
                let next = retained.checked_add(bytes).ok_or(WorkError::Overflow)?;
                work.claim_allocation(bytes, 1, copied(old))?;
                *retained = next;
                let mut grown = Vec::with_capacity(count);
                grown.append(ancestors);
                *ancestors = grown;
                work.release(old)?;
                *retained -= old;
            }
            let length = parent
                .as_os_str()
                .len()
                .checked_add(1)
                .ok_or(WorkError::Overflow)?;
            let bytes = length as u64;
            work.claim_allocation(bytes, 1, copied(bytes - 1))?;
            let mut c_path = Vec::with_capacity(length);
            c_path.extend_from_slice(parent.as_os_str().as_bytes());
            c_path.push(0);
            let hub = hub::hub()?;
            let registered = (|| {
                work.started_io(READ)?;
                let c_path = std::ffi::CStr::from_bytes_with_nul(&c_path)
                    .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
                hub.add(watch, c_path, ANCESTRAL, hub::Interest::Named(&pending))
                    .map_err(DemandError::from)
            })();
            drop(c_path);
            let released = work.release(bytes);
            let descriptor = registered?;
            // Until listed, the registration withdraws here on failure.
            let listed = released.and_then(|()| {
                let mut prior = None;
                for (index, (watched, _)) in ancestors.iter().enumerate() {
                    work.started_io(ITEM)?;
                    if *watched == descriptor {
                        prior = Some(index);
                        break;
                    }
                }
                Ok(prior)
            });
            let prior = match listed {
                Ok(prior) => prior,
                Err(error) => {
                    hub.remove(watch.subscriber, descriptor, hub::Interest::Named(&pending))?;
                    return Err(error);
                }
            };
            if let Some(index) = prior {
                let prior = ancestors
                    .get_mut(index)
                    .ok_or(DemandError::InvalidRequest)?;
                if prior.1 != pending {
                    hub.remove(watch.subscriber, descriptor, hub::Interest::Named(&prior.1))?;
                }
                let old = prior.1.capacity() as u64;
                prior.1 = std::mem::take(&mut pending);
                cleanup.record(ITEM, work)?;
                work.release(old)?;
                *retained -= old;
            } else {
                ancestors.push((descriptor, std::mem::take(&mut pending)));
            }
            temporary -= name_bytes;
            *retained = retained
                .checked_add(name_bytes)
                .ok_or(WorkError::Overflow)?;
            child = parent;
        }
        Ok(())
    })();
    drop(path);
    drop(pending);
    work.release(temporary)?;
    result
}
pub(super) fn owned(descriptor: i32) -> Result<OwnedFd, DemandError> {
    if descriptor < 0 {
        return Err(io::Error::last_os_error().into());
    }
    // SAFETY: a non-negative descriptor a system call just returned is ours.
    Ok(unsafe { OwnedFd::from_raw_fd(descriptor) })
}

impl Shared {
    pub(super) fn reserve_record_cleanup<W: SourceDemandReadWork>(
        &self,
        work: &mut W,
    ) -> Result<(), DemandError> {
        if let Some(original) = &self.delivery.original {
            self.cleanup
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(ITEM, &mut crate::NativeMaterializationWork::clone(original))?;
        } else if self.delivery.birth_matches(work) {
            self.cleanup
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .extend(ITEM, work)?;
        }
        Ok(())
    }

    pub(super) fn cancel_record_cleanup<W: SourceDemandReadWork>(&self, work: &mut W) {
        let mut cleanup = self.cleanup.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(original) = &self.delivery.original {
            cleanup.records -= 1;
            original.state.with_original_leaf(|work| {
                super::cancel_cleanup_credits(work, ITEM);
            });
        } else if self.delivery.birth_matches(work) {
            cleanup.records -= 1;
            work.with_original_leaf(|work| {
                super::cancel_cleanup_credits(work, ITEM);
            });
        }
    }
    pub(super) fn add_watch_at_with_work<W: SourceDemandReadWork>(
        &self,
        directory: i32,
        relative: &Path,
        work: &mut W,
    ) -> Result<i32, DemandError> {
        if self.stopping.load(Ordering::Acquire) {
            return Err(DemandError::Cancelled);
        }
        let (prefix, prefix_length) = proc_fd_path(directory);
        let relative = relative.as_os_str().as_bytes();
        let length = prefix_length
            .checked_add(usize::from(!relative.is_empty()))
            .and_then(|length| length.checked_add(relative.len()))
            .and_then(|length| length.checked_add(1))
            .ok_or(WorkError::Overflow)?;
        let bytes = u64::try_from(length).map_err(|_| WorkError::Overflow)?;
        let path = work.with_original_leaf(|work| {
            work.work.admit(&copied(bytes - 1), &work.budget)?;
            work.claim_allocation(
                bytes,
                u64::from((bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
            work.record(copied(bytes - 1))?;
            let mut path = Vec::with_capacity(length);
            path.extend_from_slice(prefix.split_at(prefix_length).0);
            if !relative.is_empty() {
                path.push(b'/');
                path.extend_from_slice(relative);
            }
            Ok::<_, DemandError>(path)
        })?;
        let result = (|| {
            let path =
                CString::new(path).map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            work.with_original_leaf(|work| work.started_io(READ))?;
            // Through the held root rather than a newly resolved root name.
            hub::hub()?
                .add(self, &path, WATCHED, hub::Interest::All)
                .map_err(DemandError::from)
        })();
        work.release(bytes)?;
        result
    }

    #[allow(
        clippy::too_many_lines,
        reason = "Admission keeps watch registration and its exact rollback in one frame."
    )]
    pub(super) fn admit_with_work<W: SourceDemandReadWork>(
        &self,
        directory: &Path,
        work: &mut W,
    ) -> Result<bool, DemandError> {
        work.check()?;
        if !self.delivery.is_exact() || directory.as_os_str().is_empty() {
            return Ok(false);
        }
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        if self.stopping.load(Ordering::Acquire) {
            return Ok(false);
        }
        let bytes = directory.as_os_str().len() as u64;
        let mut abandoned = false;
        let result = (|| {
            let mut prefix = work.with_original_leaf(|work| {
                work.claim_allocation(
                    bytes,
                    u64::from((bytes) != 0),
                    crate::WorkCounters::UNCHARGED,
                )?;
                Ok::<_, DemandError>(PathBuf::with_capacity(directory.as_os_str().len()))
            })?;
            let mut held: Option<(OwnedFd, u64)> = None;
            let result = (|| {
                let mut added = false;
                for component in directory.components() {
                    let std::path::Component::Normal(name) = component else {
                        return Err(DemandError::InvalidRequest);
                    };
                    work.with_original_leaf(|work| {
                        work.started_io(WorkCounters {
                            items_examined: 1,
                            bytes_copied: (component.as_os_str().len() as u64)
                                .checked_add(u64::from(!prefix.as_os_str().is_empty()))
                                .ok_or(WorkError::Overflow)?,
                            ..WorkCounters::default()
                        })?;
                        prefix.push(component);
                        Ok::<_, DemandError>(())
                    })?;
                    let parent = held.as_ref().map_or_else(
                        || self.root.load(Ordering::Acquire),
                        |(directory, _)| directory.as_raw_fd(),
                    );
                    let opened = match Self::open_component_with_work(parent, name, work) {
                        Ok(directory) => directory,
                        Err(DemandError::Io(error))
                            if matches!(
                                error.raw_os_error(),
                                Some(libc::ENOENT | libc::ENOTDIR)
                            ) =>
                        {
                            return Ok(false);
                        }
                        Err(error) => return Err(error),
                    };
                    if let Some(previous) = held.replace(opened) {
                        close_component(previous, work)?;
                    }
                    added = false;
                    if contains_path_with_work(&watches, &prefix, work)? {
                        continue;
                    }
                    self.reserve_record_cleanup(work)?;
                    let owned = match prepare_insert_with_work(
                        &mut watches,
                        &prefix,
                        work,
                        &self.retained_variable_bytes,
                        &self.ownership,
                    ) {
                        Ok(owned) => owned,
                        Err(error) => {
                            self.cancel_record_cleanup(work);
                            return Err(error);
                        }
                    };
                    let path_bytes = owned.as_os_str().len() as u64;
                    match self.add_watch_at_with_work(
                        held.as_ref()
                            .ok_or(DemandError::InvalidRequest)?
                            .0
                            .as_raw_fd(),
                        Path::new("."),
                        work,
                    ) {
                        Ok(descriptor) => {
                            let capacity = owned.capacity() as u64;
                            watches.push((owned, descriptor));

                            self.retained_variable_bytes
                                .fetch_add(capacity, Ordering::AcqRel);
                            if !self.ownership.has_native_custody() {
                                work.release(path_bytes)?;
                            }
                            added = true;
                        }
                        Err(error) => {
                            drop(owned);
                            self.cancel_record_cleanup(work);
                            self.ownership.discard_retained(path_bytes, work)?;
                            match error {
                                DemandError::Io(error)
                                    if matches!(
                                        error.raw_os_error(),
                                        Some(libc::ENOENT | libc::ENOTDIR)
                                    ) =>
                                {
                                    return Ok(false);
                                }
                                DemandError::Io(_) => {
                                    abandoned = true;
                                    return Ok(false);
                                }
                                error => return Err(error),
                            }
                        }
                    }
                }
                Ok(added)
            })();
            let closed = match held.take() {
                Some(directory) => close_component(directory, work),
                None => Ok(()),
            };
            drop(prefix);
            work.release(bytes)?;
            result.and_then(|value| closed.map(|()| value))
        })();
        drop(watches);
        if abandoned {
            self.delivery.abandon();
        }
        result
    }

    pub(super) fn unadmit_with_work<W: SourceDemandReadWork>(
        &self,
        directory: &Path,
        work: &mut W,
    ) -> Result<(), DemandError> {
        work.check()?;
        let mut watches = self.watches.lock().unwrap_or_else(PoisonError::into_inner);
        if self.stopping.load(Ordering::Acquire) {
            return Ok(());
        }
        let mut at = 0;
        while let Some((path, descriptor)) = watches.get(at) {
            work.with_original_leaf(|work| work.started_io(ITEM))?;
            if path.as_os_str().is_empty() || !path.starts_with(directory) {
                at += 1;
                continue;
            }
            let descriptor = *descriptor;
            let mut shared = false;
            for (index, (_, other)) in watches.iter().enumerate() {
                work.with_original_leaf(|work| work.started_io(ITEM))?;
                if index != at && *other == descriptor {
                    shared = true;
                    break;
                }
            }
            let copied = WorkCounters {
                bytes_copied: ((watches.len() - at - 1) as u64)
                    .checked_mul(size_of::<(PathBuf, i32)>() as u64)
                    .ok_or(WorkError::Overflow)?,
                ..WorkCounters::default()
            };
            work.with_original_leaf(|work| work.work.admit(&copied, &work.budget))?;
            if !shared {
                work.with_original_leaf(|work| work.started_io(WRITE))?;
                hub::hub()?.remove(self.subscriber, descriptor, hub::Interest::All)?;
            }
            work.record(copied)?;
            let (removed, _) = watches.remove(at);
            let bytes = removed.capacity() as u64;

            drop(removed);
            self.cancel_record_cleanup(work);
            self.ownership.retire(bytes)?;
            self.retained_variable_bytes
                .fetch_sub(bytes, Ordering::AcqRel);
        }
        Ok(())
    }

    pub(super) fn open_component_with_work<W: SourceDemandReadWork>(
        parent: i32,
        name: &OsStr,
        work: &mut W,
    ) -> Result<(OwnedFd, u64), DemandError> {
        let name = name.as_bytes();
        let capacity = name.len().checked_add(1).ok_or(WorkError::Overflow)?;
        let bytes = capacity as u64;
        let copied = copied(name.len() as u64);
        let reservation = work.reserve_cleanup_write()?;
        let buffer = match work.with_original_leaf(|work| {
            work.work.admit(&copied, &work.budget)?;
            work.claim_allocation(
                bytes,
                u64::from((bytes) != 0),
                crate::WorkCounters::UNCHARGED,
            )?;
            work.record(copied)?;
            let mut buffer = Vec::with_capacity(capacity);
            buffer.extend_from_slice(name);
            buffer.push(0);
            Ok::<_, DemandError>(buffer)
        }) {
            Ok(buffer) => buffer,
            Err(error) => {
                work.cancel_cleanup_write(reservation);
                return Err(error);
            }
        };
        let result = (|| {
            let name = CString::from_vec_with_nul(buffer)
                .map_err(|_| io::Error::from(io::ErrorKind::InvalidInput))?;
            work.with_original_leaf(|work| work.started_io(READ))?;
            // SAFETY: one normal component beneath a held directory, never following a symlink or re-resolving a previously opened prefix.
            let directory = unsafe {
                libc::openat(
                    parent,
                    name.as_ptr(),
                    libc::O_PATH | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                )
            };
            if directory < 0 {
                Err(io::Error::last_os_error().into())
            } else {
                // SAFETY: the successful open returned one exclusively owned fd.
                Ok(unsafe { OwnedFd::from_raw_fd(directory) })
            }
        })();
        let released = work.release(bytes);
        match result {
            Ok(directory) => match released {
                Ok(()) => Ok((directory, reservation)),
                Err(error) => {
                    close_component((directory, reservation), work)?;
                    Err(error)
                }
            },
            Err(error) => {
                work.cancel_cleanup_write(reservation);
                released?;
                Err(error)
            }
        }
    }
}
