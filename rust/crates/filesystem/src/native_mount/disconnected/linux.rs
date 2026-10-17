use super::*;
use rustix::fs::{AtFlags, FileType, OFlags, StatxFlags};
use std::os::fd::{AsRawFd, OwnedFd};

pub(super) struct Pinned {
    file: OwnedFd,
    parent: OwnedFd,
    path: PathBuf,
    mount: u64,
}

fn stat(file: impl std::os::fd::AsFd, flags: AtFlags) -> std::io::Result<rustix::fs::Statx> {
    #[cfg(all(test, acyclic_fs_native_loom))]
    if let Some(kernel) = super::kernel::current() {
        return kernel.stat(file.as_fd().as_raw_fd(), flags);
    }
    rustix::fs::statx(
        file,
        c"",
        flags | AtFlags::EMPTY_PATH | AtFlags::SYMLINK_NOFOLLOW,
        StatxFlags::TYPE | StatxFlags::MNT_ID,
    )
    .map_err(Into::into)
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
        if FileType::from_raw_mode(metadata.stx_mode.into()) != FileType::Directory {
            return Err(NativeMountError::InvalidDestination);
        }
        // Resolve the held object, never the possibly aliased input pathname.
        if metadata.stx_mask & StatxFlags::MNT_ID.bits() == 0 {
            return Err(NativeMountError::CapabilityUnavailable(
                "statx mount identity",
            ));
        }
        let path = std::fs::read_link(format!("/proc/self/fd/{}", parent.as_raw_fd()))?.join(leaf);
        Ok(Some(Self {
            file,
            parent,
            path,
            mount: metadata.stx_mnt_id,
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
        NativeMountRecoveryOwner::acquire_in_parent(&self.path, &self.parent)
    }

    pub(super) fn still_current(&self) -> Result<bool, NativeMountError> {
        #[cfg(all(test, acyclic_fs_native_loom))]
        if let Some(kernel) = super::kernel::current() {
            return Ok(kernel.current_stat().stx_mnt_id == self.mount);
        }
        let metadata = match rustix::fs::statx(
            &self.parent,
            self.path
                .file_name()
                .ok_or(NativeMountError::InvalidDestination)?,
            AtFlags::SYMLINK_NOFOLLOW | AtFlags::STATX_DONT_SYNC,
            StatxFlags::TYPE | StatxFlags::MNT_ID,
        ) {
            Ok(metadata) => metadata,
            Err(rustix::io::Errno::NOENT) => return Ok(false),
            Err(error) => return Err(std::io::Error::from(error).into()),
        };
        if FileType::from_raw_mode(metadata.stx_mode.into()) != FileType::Directory {
            return Err(NativeMountError::InvalidDestination);
        }
        Ok(metadata.stx_mask & StatxFlags::MNT_ID.bits() != 0 && metadata.stx_mnt_id == self.mount)
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
