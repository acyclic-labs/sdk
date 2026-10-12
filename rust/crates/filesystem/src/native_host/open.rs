use super::*;

pub(crate) enum NativeOpenOptions {
    Read,
    ReadWrite,
    CreateNew,
    ReadWriteCreateNew,
    ReadWriteCreate,
}

pub(crate) fn create_directories<W: NativeHostWork>(
    path: &Path,
    work: &mut W,
) -> Result<(), W::Error> {
    let mut base = Path::new(".");
    if path.is_absolute() {
        base = path;
        while let Some(parent) = base.parent() {
            work.started_io(ITEM)?;
            base = parent;
        }
    }
    let (directory, _) = open_root_directory(base, work)?;
    let relative = if path.is_absolute() {
        path.strip_prefix(base).map_err(|_| W::invalid_request())?
    } else {
        path
    };
    create_directories_owned(directory, relative, work).map(|_| ())
}

pub(crate) fn create_directories_held<W: NativeHostWork>(
    parent: &Dir,
    path: &Path,
    work: &mut W,
) -> Result<Dir, W::Error> {
    let directory = clone_directory(parent, work)?;
    create_directories_owned(directory, path, work)
}

fn create_directories_owned<W: NativeHostWork>(
    mut current: Dir,
    path: &Path,
    work: &mut W,
) -> Result<Dir, W::Error> {
    for component in path.components() {
        work.started_io(ITEM)?;
        let name = match component {
            std::path::Component::CurDir => continue,
            std::path::Component::Normal(name) => name,
            _ => return Err(W::invalid_request()),
        };
        #[cfg(unix)]
        {
            current = open_unix_directory(&current, name, true, work)?;
        }
        #[cfg(windows)]
        {
            current = Dir::from_std_file(with_wide_path(Path::new(name), work, |name, work| {
                create_windows_directory(&current, name, work)
            })?);
        }
    }
    Ok(current)
}

pub(crate) fn open_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &Path,
    options: &NativeOpenOptions,
    work: &mut W,
) -> Result<File, W::Error> {
    require_leaf(name)?;
    #[cfg(windows)]
    {
        with_wide_path(name, work, |name, work| {
            open_windows_leaf_with_options(parent, name, options, work)
        })
    }
    #[cfg(unix)]
    {
        let flags = libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | match options {
                NativeOpenOptions::Read => libc::O_RDONLY,
                NativeOpenOptions::ReadWrite => libc::O_RDWR,
                NativeOpenOptions::CreateNew => libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                NativeOpenOptions::ReadWriteCreateNew => {
                    libc::O_RDWR | libc::O_CREAT | libc::O_EXCL
                }
                NativeOpenOptions::ReadWriteCreate => libc::O_RDWR | libc::O_CREAT,
            };
        unix_name(name.as_os_str(), work, |name, work| {
            let file = open_unix(parent.as_raw_fd(), name, flags, work)?;
            loop {
                match fstat(file.as_raw_fd(), work) {
                    Ok(stat)
                        if stat.st_mode & libc::S_IFMT as libc::mode_t
                            == libc::S_IFREG as libc::mode_t =>
                    {
                        return Ok(file);
                    }
                    Ok(_) => return Err(W::invalid_request()),
                    Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => {
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
        })
    }
}

pub(crate) fn builtin_io(
    kind: acyclic_native_runtime::NativeOperationKind,
    maximum: usize,
    work: &NativeMaterializationWork,
    operation: impl FnOnce() -> io::Result<usize>,
) -> Result<usize, NativeTreeMaterializationError> {
    use acyclic_native_runtime::{NativeByteReservation, NativeOperationKind};
    let delta = match kind {
        NativeOperationKind::Read => READ,
        NativeOperationKind::Write => WRITE,
        _ => return Err(NativeTreeMaterializationError::InvalidLayout),
    };
    let mut transfer = NativeByteReservation::new(work.runtime_meter(), kind, maximum)
        .map_err(|error| work.runtime_error(error))?;
    let copy = work.reserve_work(copy_work(maximum as u64))?;
    work.charge(delta)?;
    let count = operation()?;
    let transferred = transfer
        .record(count)
        .map_err(|error| work.runtime_error(error));
    let copied = copy.record_completed(copy_work(count as u64));
    transferred?;
    copied?;
    Ok(count)
}

pub(crate) fn lock_builtin_file(
    file: &File,
    writable: bool,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    work.charge(READ)?;
    match if writable {
        file.try_lock()
    } else {
        file.try_lock_shared()
    } {
        Ok(()) => Ok(()),
        Err(std::fs::TryLockError::WouldBlock) => {
            Err(NativeTreeMaterializationError::PreimageConflict)
        }
        Err(std::fs::TryLockError::Error(error)) => Err(error.into()),
    }
}

pub(crate) fn remove_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &Path,
    directory: bool,
    work: &mut W,
) -> Result<(), W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    {
        unix_name(name.as_os_str(), work, |name, work| {
            remove_unix_named_leaf(parent, name, directory, work)
        })
    }
    #[cfg(windows)]
    {
        with_wide_path(name, work, |name, work| {
            remove_windows_leaf(parent, name, directory, None, work)
        })
    }
}

#[cfg(unix)]
pub(super) fn unix_mutation<W: NativeHostWork>(
    work: &mut W,
    mut operation: impl FnMut() -> libc::c_int,
) -> Result<(), W::Error> {
    loop {
        work.started_io(WRITE)?;
        if operation() == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error.into());
        }
    }
}

#[cfg(unix)]
pub(super) fn remove_unix_named_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &std::ffi::CStr,
    directory: bool,
    work: &mut W,
) -> Result<(), W::Error> {
    // SAFETY: held parent and terminated leaf outlive each unlink attempt.
    unix_mutation(work, || unsafe {
        libc::unlinkat(
            parent.as_raw_fd(),
            name.as_ptr(),
            if directory { libc::AT_REMOVEDIR } else { 0 },
        )
    })
}

#[derive(Debug)]
pub(crate) struct NativePreparedAsciiLeaf<const N: usize> {
    #[cfg(unix)]
    bytes: [u8; N],
    #[cfg(windows)]
    wide: [u16; N],
}

impl<const N: usize> NativePreparedAsciiLeaf<N> {
    pub(crate) fn new(
        bytes: [u8; N],
        work: &NativeMaterializationWork,
    ) -> Result<Self, NativeTreeMaterializationError> {
        if N < 2 || bytes[N - 1] != 0 {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        let credit = work.reserve_work(WorkCounters {
            items_examined: (N - 1) as u64,
            #[cfg(windows)]
            bytes_encoded: (N - 1).checked_mul(2).ok_or(crate::WorkError::Overflow)? as u64,
            ..WorkCounters::UNCHARGED
        })?;
        #[cfg(windows)]
        let mut wide = [0; N];
        for (index, byte) in bytes[..N - 1].iter().copied().enumerate() {
            crate::NativeMaterializationWork::admit(work, WorkCounters::UNCHARGED)?;
            #[cfg(windows)]
            {
                wide[index] = u16::from(byte);
            }
            #[cfg(unix)]
            let _ = index;
            credit.record_completed(WorkCounters {
                items_examined: 1,
                #[cfg(windows)]
                bytes_encoded: 2,
                ..WorkCounters::UNCHARGED
            })?;
            if !byte.is_ascii() || matches!(byte, 0 | b'/' | b'\\' | b':') {
                return Err(NativeTreeMaterializationError::InvalidLayout);
            }
        }
        if matches!(&bytes[..N - 1], b"." | b"..") {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        Ok(Self {
            #[cfg(unix)]
            bytes,
            #[cfg(windows)]
            wide,
        })
    }
}

pub(crate) fn remove_prepared_ascii_leaf<W: NativeHostWork, const N: usize>(
    parent: &Dir,
    name: &NativePreparedAsciiLeaf<N>,
    work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    {
        remove_unix_named_leaf(
            parent,
            std::ffi::CStr::from_bytes_with_nul(&name.bytes).map_err(|_| W::invalid_request())?,
            false,
            work,
        )
    }
    #[cfg(windows)]
    {
        remove_windows_leaf(parent, &name.wide[..N - 1], false, None, work)
    }
}
