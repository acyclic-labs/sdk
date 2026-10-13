use super::*;
use crate::materializer::NativeCleanupLoan;
use std::mem::MaybeUninit;

pub(super) struct MountProbe {
    parent: DarwinOwned<CString>,
    pub(super) mounts: DarwinOwned<Vec<MaybeUninit<libc::statfs>>>,
}

pub(super) enum ProbeWork<'a, 'b> {
    Live(&'a Work),
    Cleanup(&'a NativeCleanupLoan<'b>),
}

impl ProbeWork<'_, '_> {
    pub(super) fn original(&self) -> &Work {
        match self {
            Self::Live(work) => work,
            Self::Cleanup(loan) => loan.credit.work(),
        }
    }

    pub(super) fn check(&self) -> Result<(), NativeMountError> {
        match self {
            Self::Live(work) => work.admit(WorkCounters::UNCHARGED)?,
            Self::Cleanup(loan) => loan.check_wait()?,
        }
        Ok(())
    }

    pub(super) fn read(&self) -> Result<(), NativeMountError> {
        match self {
            Self::Live(work) => work.charge(READ_WORK)?,
            Self::Cleanup(loan) => loan.started_io(READ_WORK)?,
        }
        Ok(())
    }

    pub(super) fn copy(&self, bytes: u64) -> Result<(), NativeMountError> {
        let delta = copy_work(bytes);
        match self {
            Self::Live(work) => work.charge(delta)?,
            Self::Cleanup(loan) => loan.admit_completed(delta)?.record_completed(delta)?,
        }
        Ok(())
    }
}

impl MountProbe {
    pub(super) fn new(destination: &Path, original: &Work) -> Result<Self, NativeMountError> {
        let parent = destination
            .parent()
            .ok_or(NativeMountError::InvalidDestination)?;
        Ok(Self {
            parent: native_name(parent.as_os_str().as_bytes(), original)?,
            mounts: DarwinOwned::empty(Vec::new()),
        })
    }

    pub(super) fn preallocate(&mut self, original: &Work) -> Result<(), NativeMountError> {
        original.charge(READ_WORK)?;
        // SAFETY: a null buffer requests the current mount count.
        let count = unsafe { libc::getfsstat(ptr::null_mut(), 0, libc::MNT_NOWAIT) };
        if count < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        // Reserve the future mount and a truncation sentinel before birth.
        self.mounts = vector(
            original,
            (count as usize).checked_add(2).ok_or(WorkError::Overflow)?,
        )?;
        Ok(())
    }

    pub(super) fn mounted(
        &mut self,
        destination: &Path,
        work: &ProbeWork<'_, '_>,
    ) -> Result<bool, NativeMountError> {
        let mut path = [0_u8; libc::PATH_MAX as usize];
        let mut close = match work {
            ProbeWork::Cleanup(loan) => loan.admit_completed(WRITE_WORK)?,
            ProbeWork::Live(original) => original.reserve_work(WRITE_WORK)?,
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
            let mut length = CStr::from_bytes_until_nul(&path)
                .map_err(|_| crate::NativeTreeMaterializationError::InvalidLayout)?
                .to_bytes()
                .len();
            work.check()?;
            let name = destination
                .file_name()
                .ok_or(NativeMountError::InvalidDestination)?
                .as_bytes();
            let separator = usize::from(length != 0 && path[length - 1] != b'/');
            let end = length
                .checked_add(separator)
                .and_then(|n| n.checked_add(name.len()))
                .ok_or(WorkError::Overflow)?;
            if end > path.len() {
                return Err(driver_errno(libc::ENAMETOOLONG));
            }
            work.copy(name.len() as u64)?;
            if separator != 0 {
                path[length] = b'/';
                length += 1;
            }
            path[length..end].copy_from_slice(name);

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
        let normalized = &path[..end];
        if self.mounts.capacity() == 0 {
            self.preallocate(work.original())?;
        }
        loop {
            let bytes = self
                .mounts
                .capacity()
                .checked_mul(std::mem::size_of::<libc::statfs>())
                .and_then(|n| c_int::try_from(n).ok())
                .ok_or(WorkError::Overflow)?;
            work.read()?;
            // SAFETY: bytes is the funded writable backing capacity.
            let count = unsafe {
                libc::getfsstat(self.mounts.as_mut_ptr().cast(), bytes, libc::MNT_NOWAIT)
            };
            if count < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            if count as usize >= self.mounts.capacity() {
                if matches!(work, ProbeWork::Cleanup(_)) {
                    return Err(NativeMountError::Lifecycle(
                        "Darwin mount table exceeds its prepaid capacity",
                    ));
                }
                self.preallocate(work.original())?;
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
                work.check()?;
                // SAFETY: Darwin terminates f_mntonname.
                let name = unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) }.to_bytes();
                if name == destination.as_os_str().as_bytes() || name == normalized {
                    return Ok(true);
                }
            }
            return Ok(false);
        }
    }
}
