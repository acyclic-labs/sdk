use super::*;
use crate::materializer::NativeCleanupLoan;
use std::mem::MaybeUninit;

pub(super) struct MountProbe {
    parent: DarwinOwned<CString>,
    normalized: DarwinOwned<Vec<MaybeUninit<u8>>>,
    pub(super) mounts: DarwinOwned<Vec<MaybeUninit<libc::statfs>>>,
}

pub(super) struct ProbeWork<'a, 'b> {
    pub(super) original: &'a crate::NativeMaterializationWork,
    pub(super) cleanup: Option<&'a NativeCleanupLoan<'b>>,
}

impl ProbeWork<'_, '_> {
    pub(super) fn check(&self) -> Result<(), NativeMountError> {
        match self.cleanup {
            Some(cleanup) => cleanup.check_wait()?,
            None => self.original.admit(crate::WorkCounters::UNCHARGED)?,
        }
        Ok(())
    }

    pub(super) fn read(&self) -> Result<(), NativeMountError> {
        let delta = READ_WORK;
        match self.cleanup {
            Some(cleanup) => cleanup.started_io(delta)?,
            None => self.original.charge(delta)?,
        }
        Ok(())
    }

    pub(super) fn cpu<T>(
        &self,
        delta: crate::WorkCounters,
        operation: impl FnOnce() -> T,
    ) -> Result<T, NativeMountError> {
        if let Some(cleanup) = self.cleanup {
            let mut credit = cleanup.admit_completed(delta)?;
            let result = operation();
            credit.record_completed(delta)?;
            Ok(result)
        } else {
            copying(self.original, delta, operation)
        }
    }
}

impl MountProbe {
    pub(super) fn new(
        destination: &Path,
        original: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let parent = destination
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?;
        let mut probe = Self {
            parent: native_name(parent.as_os_str().as_bytes(), original)?,
            normalized: DarwinOwned::empty(Vec::new()),
            mounts: DarwinOwned::empty(Vec::new()),
        };
        reserve_buffer(&mut probe.normalized, libc::PATH_MAX as usize, original)?;
        // SAFETY: MaybeUninit has no initialization requirement.
        unsafe {
            probe.normalized.set_len(libc::PATH_MAX as usize);
        }
        Ok(probe)
    }

    pub(super) fn preallocate(
        &mut self,
        original: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        original.charge(READ_WORK)?;
        // SAFETY: a null buffer requests the current mount count.
        let count = unsafe { libc::getfsstat(ptr::null_mut(), 0, libc::MNT_NOWAIT) };
        if count < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // The driver's future namespace and a truncation sentinel.
        let capacity = (count as usize)
            .checked_add(2)
            .ok_or(crate::WorkError::Overflow)?;
        reserve_buffer(&mut self.mounts, capacity, original)?;
        Ok(())
    }

    pub(super) fn mounted(
        &mut self,
        destination: &Path,
        work: &ProbeWork<'_, '_>,
    ) -> Result<bool, NativeMountError> {
        let mut close = match work.cleanup {
            Some(cleanup) => cleanup.admit_completed(WRITE_WORK)?,
            None => work.original.reserve_work(WRITE_WORK)?,
        };
        work.read()?;
        // SAFETY: parent is terminated and stays owned through this call.
        let fd = unsafe { libc::open(self.parent.as_ptr(), libc::O_EVTONLY | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let normalized = self.normalize(destination, fd, work);
        close.started_drop_io(WRITE_WORK);
        // SAFETY: this call closes exactly the descriptor opened above.
        let closed = unsafe { libc::close(fd) };
        let close_error = (closed != 0).then(std::io::Error::last_os_error);
        let end = normalized?;
        if let Some(error) = close_error {
            return Err(error.into());
        }
        loop {
            work.read()?;
            // SAFETY: a null buffer requests the current mount count.
            let count = unsafe { libc::getfsstat(ptr::null_mut(), 0, libc::MNT_NOWAIT) };
            if count < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let required = (count as usize)
                .checked_add(1)
                .ok_or(crate::WorkError::Overflow)?;
            if required > self.mounts.capacity() {
                if work.cleanup.is_some() {
                    return Err(NativeMountError::DarwinTeardown(
                        DarwinTeardownFailure::MountTableCapacityExceeded,
                    ));
                }
                reserve_buffer(&mut self.mounts, required, work.original)?;
            }
            let bytes = self
                .mounts
                .capacity()
                .checked_mul(std::mem::size_of::<libc::statfs>())
                .and_then(|bytes| c_int::try_from(bytes).ok())
                .ok_or(crate::WorkError::Overflow)?;
            work.read()?;
            // SAFETY: bytes is the funded writable backing capacity.
            let count = unsafe {
                libc::getfsstat(self.mounts.as_mut_ptr().cast(), bytes, libc::MNT_NOWAIT)
            };
            if count < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if count as usize >= self.mounts.capacity() {
                if work.cleanup.is_some() {
                    return Err(NativeMountError::DarwinTeardown(
                        DarwinTeardownFailure::MountTableCapacityExceeded,
                    ));
                }
                reserve_buffer(
                    &mut self.mounts,
                    (count as usize)
                        .checked_add(1)
                        .ok_or(crate::WorkError::Overflow)?,
                    work.original,
                )?;
                continue;
            }
            // SAFETY: the successful fill initialized exactly count entries.
            let table = unsafe {
                std::slice::from_raw_parts(
                    self.mounts.as_ptr().cast::<libc::statfs>(),
                    count as usize,
                )
            };
            // SAFETY: normalize initialized the complete returned prefix.
            let normalized =
                unsafe { std::slice::from_raw_parts(self.normalized.as_ptr().cast::<u8>(), end) };
            for mount in table {
                if work.cpu(scan_work(1), || {
                    // SAFETY: Darwin terminates f_mntonname.
                    let name = unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) }.to_bytes();
                    name == destination.as_os_str().as_bytes() || name == normalized
                })? {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }

    fn normalize(
        &mut self,
        destination: &Path,
        fd: c_int,
        work: &ProbeWork<'_, '_>,
    ) -> Result<usize, NativeMountError> {
        work.read()?;
        let mut metadata = MaybeUninit::<libc::stat>::uninit();
        // SAFETY: fd is open and metadata is writable.
        if unsafe { libc::fstat(fd, metadata.as_mut_ptr()) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        work.read()?;
        // SAFETY: F_GETPATH writes at most PATH_MAX bytes into this backing.
        if unsafe { libc::fcntl(fd, libc::F_GETPATH, self.normalized.as_mut_ptr()) } < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let path = &mut self.normalized;
        let mut length = 0;
        for byte in path.iter() {
            if work.cpu(scan_work(1), || unsafe { byte.assume_init() } == 0)? {
                break;
            }
            length += 1;
        }
        if length == path.len() {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout.into());
        }
        let name = destination
            .file_name()
            .ok_or(NativeMountError::InvalidDestination)?
            .as_bytes();
        let separator =
            usize::from(length != 0 && unsafe { path[length - 1].assume_init() } != b'/');
        let end = length
            .checked_add(separator)
            .and_then(|n| n.checked_add(name.len()))
            .ok_or(crate::WorkError::Overflow)?;
        if end > path.len() {
            return Err(driver_errno(libc::ENAMETOOLONG));
        }
        work.cpu(copy_work(name.len() as u64), || {
            if separator != 0 {
                path[length].write(b'/');
                length += 1;
            }
            // SAFETY: the checked destination range fits the funded buffer.
            unsafe {
                ptr::copy_nonoverlapping(
                    name.as_ptr(),
                    path.as_mut_ptr().add(length).cast::<u8>(),
                    name.len(),
                );
            }
        })?;
        Ok(end)
    }
}

fn reserve_buffer<T>(
    buffer: &mut DarwinOwned<Vec<MaybeUninit<T>>>,
    count: usize,
    work: &crate::NativeMaterializationWork,
) -> Result<(), NativeMountError> {
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or(crate::WorkError::Overflow)?;
    let mut replacement = owned(work, bytes as u64, || {
        let mut value = Vec::new();
        value.try_reserve_exact(count)?;
        Ok(value)
    })?;
    if let Some(allocation) = &mut replacement.allocation {
        allocation
            .reconcile_capacity((replacement.value.capacity() * std::mem::size_of::<T>()) as u64)?;
    }
    drop(std::mem::replace(buffer, replacement));
    Ok(())
}
