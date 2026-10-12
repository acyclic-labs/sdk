use super::*;
use crate::materializer::NativeCleanupLoan;
use std::mem::MaybeUninit;

pub(super) struct MountProbe {
    parent: DarwinOwned<CString>,
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
            self.original.charge(delta)?;
            Ok(operation())
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
        Ok(Self {
            parent: native_name(parent.as_os_str().as_bytes(), original)?,
            mounts: DarwinOwned::empty(Vec::new()),
        })
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
        // Reserve the future mount and a truncation sentinel before birth.
        self.mounts = vector(
            original,
            (count as usize)
                .checked_add(2)
                .ok_or(crate::WorkError::Overflow)?,
        )?;
        Ok(())
    }

    pub(super) fn mounted(
        &mut self,
        destination: &Path,
        work: &ProbeWork<'_, '_>,
    ) -> Result<bool, NativeMountError> {
        let mut path = [MaybeUninit::<u8>::uninit(); libc::PATH_MAX as usize];
        let mut close = match work.cleanup {
            Some(cleanup) => cleanup.admit_completed(WRITE_WORK)?,
            None => work.original.reserve_work(WRITE_WORK)?,
        };
        work.read()?;
        // SAFETY: parent is terminated and retained through open.
        let fd = unsafe { libc::open(self.parent.as_ptr(), libc::O_EVTONLY | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let normalized = (|| {
            work.read()?;
            // SAFETY: F_GETPATH initializes the path through its terminator in PATH_MAX bytes.
            if unsafe { libc::fcntl(fd, libc::F_GETPATH, path.as_mut_ptr()) } < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let mut length = 0;
            for byte in &path {
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
                for (slot, byte) in path[length..end].iter_mut().zip(name) {
                    slot.write(*byte);
                }
            })?;
            Ok::<_, NativeMountError>(end)
        })();
        close.started_drop_io(WRITE_WORK);
        // SAFETY: closes exactly the descriptor opened above, including error paths.
        let closed = unsafe { libc::close(fd) };
        let close_error = (closed != 0).then(std::io::Error::last_os_error);
        let end = normalized?;
        if let Some(error) = close_error {
            return Err(error.into());
        }
        // SAFETY: the prefix contains the initialized parent and appended name.
        let normalized = unsafe { std::slice::from_raw_parts(path.as_ptr().cast::<u8>(), end) };
        if self.mounts.capacity() == 0 {
            self.preallocate(work.original)?;
        }
        loop {
            let bytes = self
                .mounts
                .capacity()
                .checked_mul(std::mem::size_of::<libc::statfs>())
                .and_then(|n| c_int::try_from(n).ok())
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
                    return Err(NativeMountError::Lifecycle(
                        "Darwin mount table exceeds its prepaid capacity",
                    ));
                }
                self.preallocate(work.original)?;
                continue;
            }
            // SAFETY: successful fill initialized exactly count entries.
            let table = unsafe {
                std::slice::from_raw_parts(
                    self.mounts.as_ptr().cast::<libc::statfs>(),
                    count as usize,
                )
            };
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
}
