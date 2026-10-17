use super::*;
use rustix::fs::{AtFlags, FileType, OFlags, StatxFlags};
use std::os::fd::{AsRawFd, OwnedFd};

pub(super) struct Pinned {
    file: OwnedFd,
    parent: Arc<OwnedFd>,
    path: PathBuf,
    mount: u64,
}

fn stat(file: impl std::os::fd::AsFd, flags: AtFlags) -> std::io::Result<rustix::fs::Statx> {
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
            parent: Arc::new(parent),
            path,
            mount: metadata.stx_mnt_id,
        }))
    }
}

impl Destination for Pinned {
    type Fence<'a> = NativeMountRecoveryOwner;

    fn disconnected(&self) -> Result<bool, NativeMountError> {
        match stat(&self.file, AtFlags::STATX_FORCE_SYNC) {
            Ok(_) => Ok(false),
            Err(error) if error.kind() == std::io::ErrorKind::NotConnected => Ok(true),
            Err(error) => Err(error.into()),
        }
    }

    fn acquire(&self) -> Result<Self::Fence<'_>, NativeMountError> {
        // Acquire inside the already held physical parent, not a newly
        // resolved parent pathname that could now name a different directory.
        let parent = PathBuf::from(format!(
            "/proc/self/fd/{}",
            self.parent.as_ref().as_raw_fd()
        ));
        NativeMountRecoveryOwner::acquire(
            &parent.join(".").join(
                self.path
                    .file_name()
                    .ok_or(NativeMountError::InvalidDestination)?,
            ),
        )
    }

    fn still_current(&self) -> Result<bool, NativeMountError> {
        let metadata = match rustix::fs::statx(
            self.parent.as_ref(),
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

    fn detach(&self, mut fence: Self::Fence<'_>) -> Result<(), NativeMountError> {
        let work = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| NativeMountError::from(error))?;
        let file = rustix::io::dup(&self.file).map_err(std::io::Error::from)?;
        match fence.detach_pinned(file.into(), self.parent.clone(), &work) {
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
