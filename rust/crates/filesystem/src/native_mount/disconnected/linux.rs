use super::*;
use rustix::fs::{AtFlags, FileType, OFlags, StatxFlags};
use std::os::fd::{AsRawFd, OwnedFd};

pub(super) struct Pinned {
    file: OwnedFd,
    parent: OwnedFd,
    path: PathBuf,
    mount: Option<u64>,
}

fn stat(file: &OwnedFd, flags: AtFlags) -> std::io::Result<(FileType, Option<u64>)> {
    #[cfg(all(test, acyclic_fs_native_loom))]
    if let Some(kernel) = super::kernel::current() {
        return metadata(file, flags, kernel.stat(file.as_raw_fd(), flags), || {
            kernel
                .stat(file.as_raw_fd(), AtFlags::STATX_FORCE_SYNC)
                .map(|_| ())
        });
    }
    let result = rustix::fs::statx(
        file,
        c"",
        flags | AtFlags::EMPTY_PATH | AtFlags::SYMLINK_NOFOLLOW,
        StatxFlags::TYPE | StatxFlags::MNT_ID,
    )
    .map_err(Into::into);
    metadata(file, flags, result, || {
        rustix::fs::fstatfs(file).map(|_| ()).map_err(Into::into)
    })
}

fn metadata(
    file: &OwnedFd,
    flags: AtFlags,
    result: std::io::Result<rustix::fs::Statx>,
    fresh_probe: impl FnOnce() -> std::io::Result<()>,
) -> std::io::Result<(FileType, Option<u64>)> {
    match result {
        Ok(metadata) => Ok((
            FileType::from_raw_mode(metadata.stx_mode.into()),
            (metadata.stx_mask & StatxFlags::MNT_ID.bits() != 0).then_some(metadata.stx_mnt_id),
        )),
        Err(error) if matches!(error.raw_os_error(), Some(libc::EPERM | libc::ENOSYS)) => {
            // fstat may return cached FUSE attributes after disconnect. Only a
            // fresh operation on this same descriptor may establish liveness.
            if flags.contains(AtFlags::STATX_FORCE_SYNC) {
                fresh_probe()?;
            }
            let metadata = rustix::fs::fstat(file)?;
            Ok((FileType::from_raw_mode(metadata.st_mode), None))
        }
        Err(error) => Err(error),
    }
}

impl Pinned {
    pub(super) fn open(path: &Path) -> Result<Option<Self>, NativeMountError> {
        let parent = path.parent().ok_or(NativeMountError::InvalidDestination)?;
        let leaf = path
            .file_name()
            .ok_or(NativeMountError::InvalidDestination)?;
        let parent = rustix::fs::open(
            parent,
            OFlags::PATH | OFlags::DIRECTORY | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(std::io::Error::from)?;
        Self::pin(parent, leaf)
    }

    /// Pins `leaf` in an already held parent directory.
    pub(super) fn open_in(
        parent: &impl std::os::fd::AsFd,
        leaf: &std::ffi::OsStr,
    ) -> Result<Option<Self>, NativeMountError> {
        crate::native_host::require_leaf(Path::new(leaf))
            .map_err(|_| NativeMountError::InvalidDestination)?;
        let parent = rustix::fs::openat(
            parent,
            c".",
            OFlags::PATH | OFlags::DIRECTORY | OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        )
        .map_err(std::io::Error::from)?;
        Self::pin(parent, leaf)
    }

    fn pin(parent: OwnedFd, leaf: &std::ffi::OsStr) -> Result<Option<Self>, NativeMountError> {
        let file = match rustix::fs::openat(
            &parent,
            leaf,
            OFlags::PATH | OFlags::NOFOLLOW | OFlags::CLOEXEC,
            rustix::fs::Mode::empty(),
        ) {
            Ok(file) => file,
            Err(rustix::io::Errno::NOENT) => return Ok(None),
            Err(error) => return Err(std::io::Error::from(error).into()),
        };
        let metadata = stat(&file, AtFlags::STATX_DONT_SYNC)?;
        if metadata.0 != FileType::Directory {
            return Err(NativeMountError::InvalidDestination);
        }
        // Resolve the held object, never the possibly aliased input pathname.
        let path = std::fs::read_link(format!("/proc/self/fd/{}", parent.as_raw_fd()))?.join(leaf);
        Ok(Some(Self {
            file,
            parent,
            path,
            mount: metadata.1,
        }))
    }
}

impl Pinned {
    pub(super) fn disconnected(&self) -> Result<bool, NativeMountError> {
        match stat(&self.file, AtFlags::STATX_FORCE_SYNC) {
            Ok(_) => Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotConnected => Ok(true),
            Err(error) => Err(error.into()),
        }
    }

    pub(super) fn acquire(&self) -> Result<NativeMountRecoveryOwner, NativeMountError> {
        // Healthy destinations never require recovery-only mount identity.
        if self.mount.is_none() {
            return Err(NativeMountError::CapabilityUnavailable(
                "statx mount identity",
            ));
        }
        NativeMountRecoveryOwner::acquire_in_parent(&self.path, &self.parent)
    }

    pub(super) fn still_current(&self) -> Result<bool, NativeMountError> {
        let metadata = match self.current_stat() {
            Ok(metadata) => metadata,
            Err(rustix::io::Errno::NOENT) => return Ok(false),
            Err(error) => return Err(std::io::Error::from(error).into()),
        };
        if FileType::from_raw_mode(metadata.stx_mode.into()) != FileType::Directory {
            return Err(NativeMountError::InvalidDestination);
        }
        Ok(metadata.stx_mask & StatxFlags::MNT_ID.bits() != 0
            && Some(metadata.stx_mnt_id) == self.mount)
    }

    fn current_stat(&self) -> rustix::io::Result<rustix::fs::Statx> {
        #[cfg(all(test, acyclic_fs_native_loom))]
        if let Some(kernel) = super::kernel::current() {
            return Ok(kernel.current_stat());
        }
        rustix::fs::statx(
            &self.parent,
            self.path.file_name().ok_or(rustix::io::Errno::INVAL)?,
            AtFlags::SYMLINK_NOFOLLOW | AtFlags::STATX_DONT_SYNC,
            StatxFlags::TYPE | StatxFlags::MNT_ID,
        )
    }

    pub(super) fn detach(
        &self,
        mut fence: NativeMountRecoveryOwner,
    ) -> Result<(), NativeMountError> {
        let work = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| NativeMountError::from(error))?;
        work.charge(crate::materializer::NativeResourceCredit::ReadOperations.delta(2))?;
        let file = self.file.try_clone()?;
        let parent = self.parent.try_clone()?;
        match fence.detach_pinned(file.into(), parent.into(), &work) {
            Ok(_) => Ok(()),
            Err(error) => {
                fence.mark_failed();
                Err(NativeMountError::RecoveryFailed {
                    error: Box::new(error),
                    owner: fence,
                })
            }
        }
    }
}

#[cfg(test)]
#[cfg(not(acyclic_fs_native_loom))]
mod tests;
