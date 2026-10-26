use super::*;

pub(super) static MOUNT_TABLE: Mutex<()> = Mutex::new(());
pub(super) struct MountProbe(pub(super) Arc<DarwinOwned<CString>>, Option<libc::statfs>);

pub(super) fn same_mount(left: &libc::statfs, right: &libc::statfs) -> bool {
    // SAFETY: Darwin's fsid_t consists of two initialized int32_t values.
    let identity = |mount: &libc::statfs| unsafe {
        std::mem::transmute::<libc::fsid_t, [i32; 2]>(mount.f_fsid)
    };
    identity(left) == identity(right)
        && left.f_owner == right.f_owner
        && left.f_mntonname == right.f_mntonname
        && left.f_mntfromname == right.f_mntfromname
}

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
    pub(super) fn verify_mount(&self, current: &libc::statfs) -> Result<(), NativeMountError> {
        if self
            .1
            .as_ref()
            .is_some_and(|expected| !same_mount(expected, current))
        {
            return Err(NativeMountError::Demand(crate::DemandError::StaleSource));
        }
        Ok(())
    }

    pub(super) fn for_mount(mount: &libc::statfs, work: &Work) -> Result<Self, NativeMountError> {
        let destination = super::reclaim::field(&mount.f_mntonname)?;
        Ok(Self(
            shared(work, native_name(destination.to_bytes(), work)?)?,
            Some(*mount),
        ))
    }

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
            let parent = CStr::from_bytes_until_nul(&path)
                .map_err(|_| driver_errno(libc::EIO))?
                .to_bytes();
            let length = parent.len();
            let name = destination
                .file_name()
                .ok_or(NativeMountError::InvalidDestination)?
                .as_bytes();
            let separator = usize::from(parent.last().is_some_and(|byte| *byte != b'/'));
            let end = length
                .checked_add(separator)
                .and_then(|n| n.checked_add(name.len()))
                .ok_or(WorkError::Overflow)?;
            if end >= path.len() {
                return Err(driver_errno(libc::ENAMETOOLONG));
            }
            work.charge(copy_work(name.len() as u64))?;
            let path = path
                .get_mut(..end)
                .ok_or_else(|| driver_errno(libc::ENAMETOOLONG))?;
            let suffix = path
                .get_mut(length..)
                .ok_or_else(|| driver_errno(libc::ENAMETOOLONG))?;
            let suffix = if separator != 0 {
                let (slash, suffix) = suffix
                    .split_first_mut()
                    .ok_or_else(|| driver_errno(libc::ENAMETOOLONG))?;
                *slash = b'/';
                suffix
            } else {
                suffix
            };
            suffix.copy_from_slice(name);
            shared(work, native_name(path, work)?).map(|path| Self(path, None))
        })();
        close.started_drop_io(WRITE_WORK);
        // SAFETY: closes the descriptor opened here on every result path.
        let closed = unsafe { libc::close(fd) };
        match normalized {
            Ok(_value) if closed != 0 => Err(std::io::Error::last_os_error().into()),
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
        let count = usize::try_from(count).map_err(|_| driver_errno(libc::EOVERFLOW))?;
        // SAFETY: positive count is the initialized table length, retained under MOUNT_TABLE.
        let mounts = unsafe { std::slice::from_raw_parts(mounts, count) };
        for mount in mounts {
            work.check()?;
            // SAFETY: Darwin terminates f_mntonname.
            let name = unsafe { CStr::from_ptr(mount.f_mntonname.as_ptr()) }.to_bytes();
            if name == destination.as_os_str().as_bytes() || name == self.0.as_bytes() {
                self.verify_mount(mount)?;
                return Ok(true);
            }
        }
        Ok(false)
    }
}
