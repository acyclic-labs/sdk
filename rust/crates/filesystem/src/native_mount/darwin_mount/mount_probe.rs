use super::*;

pub(super) static MOUNT_TABLE: Mutex<()> = Mutex::new(());
pub(super) struct MountProbe(pub(super) Arc<DarwinOwned<CString>>);

pub(super) enum ProbeWork<'a> {
    Live(&'a Work),
    Cleanup(
        &'a crate::NativeWorkReservation,
        Option<&'a crate::CancellationToken>,
    ),
}

impl ProbeWork<'_> {
    pub(super) fn original(&self) -> &Work {
        match self {
            Self::Live(work) => work,
            Self::Cleanup(credit, _) => credit.work(),
        }
    }
    pub(super) fn check(&self) -> Result<(), NativeMountError> {
        match self {
            Self::Live(work) => work.admit(WorkCounters::UNCHARGED)?,
            Self::Cleanup(credit, wait) => credit.check_wait(*wait)?,
        }
        Ok(())
    }
    pub(super) fn read(&self) -> Result<(), NativeMountError> {
        match self {
            Self::Live(work) => work.charge(READ_WORK)?,
            Self::Cleanup(credit, wait) => credit.started_io(READ_WORK, *wait)?,
        }
        Ok(())
    }
}

impl MountProbe {
    pub(super) fn new(destination: &Path, work: &Work) -> Result<Self, NativeMountError> {
        let parent = native_name(
            destination
                .parent()
                .ok_or(NativeMountError::InvalidDestination)?
                .as_os_str()
                .as_bytes(),
            work,
        )?;
        let close = work.reserve_work(WRITE_WORK)?;
        work.charge(READ_WORK)?;
        // SAFETY: parent is terminated and remains live through open.
        let fd = unsafe { libc::open(parent.as_ptr(), libc::O_EVTONLY | libc::O_CLOEXEC) };
        if fd < 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        let normalized = (|| {
            let mut path = [0_u8; libc::PATH_MAX as usize];
            work.charge(READ_WORK)?;
            // SAFETY: F_GETPATH writes a terminated path into PATH_MAX bytes.
            if unsafe { libc::fcntl(fd, libc::F_GETPATH, path.as_mut_ptr()) } < 0 {
                return Err(std::io::Error::last_os_error().into());
            }
            let length = CStr::from_bytes_until_nul(&path)
                .map_err(|_| driver_errno(libc::EIO))?
                .to_bytes()
                .len();
            let name = destination
                .file_name()
                .ok_or(NativeMountError::InvalidDestination)?
                .as_bytes();
            let separator = usize::from(length != 0 && path[length - 1] != b'/');
            let end = length
                .checked_add(separator)
                .and_then(|n| n.checked_add(name.len()))
                .ok_or(WorkError::Overflow)?;
            if end >= path.len() {
                return Err(driver_errno(libc::ENAMETOOLONG));
            }
            work.charge(copy_work(name.len() as u64))?;
            if separator != 0 {
                path[length] = b'/';
            }
            path[length + separator..end].copy_from_slice(name);
            shared(work, native_name(&path[..end], work)?).map(Self)
        })();
        close.started_drop_io(WRITE_WORK);
        // SAFETY: closes the descriptor opened here on every result path.
        let closed = unsafe { libc::close(fd) };
        match normalized {
            Ok(value) if closed != 0 => Err(std::io::Error::last_os_error().into()),
            result => result,
        }
    }

    pub(super) fn mounted(
        &self,
        destination: &Path,
        work: &ProbeWork<'_>,
    ) -> Result<bool, NativeMountError> {
        let _table = MOUNT_TABLE.lock().unwrap_or_else(PoisonError::into_inner);
        work.read()?;
        let mut mounts = ptr::null_mut();
        // SAFETY: libc owns the returned table; serialized SDK queries consume it before the next call.
        let count = unsafe { libc::getmntinfo(&raw mut mounts, libc::MNT_NOWAIT) };
        if count <= 0 || mounts.is_null() {
            return Err(std::io::Error::last_os_error().into());
        }
        // SAFETY: positive count is the initialized table length, retained under MOUNT_TABLE.
        let mounts = unsafe { std::slice::from_raw_parts(mounts, count as usize) };
        for mount in mounts {
            work.check()?;
            // SAFETY: Darwin terminates f_mntonname.
            let name = unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) }.to_bytes();
            if name == destination.as_os_str().as_bytes() || name == self.0.as_bytes() {
                return Ok(true);
            }
        }
        Ok(false)
    }
}
