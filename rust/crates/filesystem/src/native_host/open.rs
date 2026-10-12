use super::*;

pub(crate) enum NativeOpenOptions {
    Read,
    ReadWrite,
    CreateNew,
    /// Creates only an absent regular leaf, retaining a read/write descriptor.
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
            let delta = ITEM;
            work.admit(delta)?;
            work.record(delta)?;
            base = parent;
        }
    }
    let current = open_root_directory(base, work).map(|(directory, _)| directory)?;
    let relative = if path.is_absolute() {
        path.strip_prefix(base).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "invalid materialization root")
        })?
    } else {
        path
    };
    create_directories_owned(current, relative, work).map(|_| ())
}

pub(crate) fn create_directories_held<W: NativeHostWork>(
    parent: &Dir,
    relative: &Path,
    work: &mut W,
) -> Result<Dir, W::Error> {
    let current = clone_directory(parent, work)?;
    create_directories_owned(current, relative, work)
}

pub(crate) fn create_new_directory_held<W: NativeHostWork>(
    parent: &Dir,
    leaf: &Path,
    work: &mut W,
) -> Result<Dir, W::Error> {
    require_leaf(leaf)?;
    #[cfg(unix)]
    let directory = unix_name(leaf.as_os_str(), work, |leaf, work| {
        create_unix_directory_leaf(parent, leaf, work)?;
        let file = open_unix(
            parent.as_raw_fd(),
            leaf,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            work,
        )?;
        Ok(Dir::from_std_file(file))
    })?;
    #[cfg(windows)]
    let directory = with_wide_path(leaf, work, |leaf, work| {
        let file = create_windows_wide_at(parent, leaf, true, false, false, false, work)?;
        Ok(Dir::from_std_file(file))
    })?;
    sync_directory_held(parent, work)?;
    Ok(directory)
}

pub(super) fn create_directories_owned<W: NativeHostWork>(
    mut current: Dir,
    relative: &Path,
    work: &mut W,
) -> Result<Dir, W::Error> {
    for component in relative.components() {
        let delta = ITEM;
        work.admit(delta)?;
        work.record(delta)?;
        let name = match component {
            std::path::Component::CurDir => continue,
            std::path::Component::Normal(name) => name,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid held materialization directory component",
                )
                .into());
            }
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
    #[cfg(unix)]
    {
        let flags = libc::O_CLOEXEC
            | libc::O_NOFOLLOW
            | libc::O_NONBLOCK
            | match options {
                NativeOpenOptions::Read => libc::O_RDONLY,
                NativeOpenOptions::CreateNew => libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
                NativeOpenOptions::ReadWrite => libc::O_RDWR,
                NativeOpenOptions::ReadWriteCreateNew => {
                    libc::O_RDWR | libc::O_CREAT | libc::O_EXCL
                }
                NativeOpenOptions::ReadWriteCreate => libc::O_RDWR | libc::O_CREAT,
            };
        unix_name(name.as_os_str(), work, |name, work| {
            let file = open_unix(parent.as_raw_fd(), name, flags, work)?;
            loop {
                let result = work.attempt_io(READ, || {
                    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
                    // SAFETY: held descriptor and complete output storage remain live.
                    if unsafe { libc::fstat(file.as_raw_fd(), stat.as_mut_ptr()) } != 0 {
                        return Err(io::Error::last_os_error());
                    }
                    Ok(unsafe { stat.assume_init() })
                });
                match result {
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
    #[cfg(windows)]
    {
        with_wide_path(name, work, |name, work| {
            open_windows_leaf_with_options(parent, name, options, work)
        })
    }
}

pub(crate) fn open_builtin_directory<W: NativeHostWork>(
    parent: &Dir,
    name: &Path,
    create: bool,
    work: &mut W,
) -> Result<Dir, W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    {
        open_unix_directory(parent, name.as_os_str(), create, work)
    }
    #[cfg(windows)]
    {
        with_wide_path(name, work, |name, work| {
            if create {
                return create_windows_directory(parent, name, work).map(Dir::from_std_file);
            }
            use windows::Win32::Storage::FileSystem::FILE_GENERIC_READ;
            open_windows_leaf_handle(parent, name, FILE_GENERIC_READ, Some(true), work)
                .map(Dir::from_std_file)
        })
    }
}

pub(crate) fn read_builtin_file(
    file: &File,
    destination: &mut [u8],
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<usize, crate::materializer::NativeTreeMaterializationError> {
    use std::io::Read as _;
    let meter = work.runtime_meter();
    let mut transfer = acyclic_native_runtime::NativeByteReservation::new(
        meter.as_ref(),
        acyclic_native_runtime::NativeOperationKind::Read,
        destination.len(),
    )
    .map_err(native_builtin_runtime_error)?;
    let copy = work.reserve_work(crate::WorkCounters {
        bytes_copied: destination.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    work.charge(READ)?;
    let count = (&*file).read(destination)?;
    let transferred = transfer.record(count).map_err(native_builtin_runtime_error);
    let copied = {
        let mut copy = copy;
        copy.record_completed(crate::WorkCounters {
            bytes_copied: count as u64,
            ..crate::WorkCounters::UNCHARGED
        })
    };
    transferred?;
    copied?;
    Ok(count)
}

pub(crate) fn write_builtin_file(
    file: &File,
    source: &[u8],
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<usize, crate::materializer::NativeTreeMaterializationError> {
    use std::io::Write as _;
    let meter = work.runtime_meter();
    let mut transfer = acyclic_native_runtime::NativeByteReservation::new(
        meter.as_ref(),
        acyclic_native_runtime::NativeOperationKind::Write,
        source.len(),
    )
    .map_err(native_builtin_runtime_error)?;
    let copy = work.reserve_work(crate::WorkCounters {
        bytes_copied: source.len() as u64,
        ..crate::WorkCounters::UNCHARGED
    })?;
    work.charge(WRITE)?;
    let count = (&*file).write(source)?;
    let transferred = transfer.record(count).map_err(native_builtin_runtime_error);
    let copied = {
        let mut copy = copy;
        copy.record_completed(crate::WorkCounters {
            bytes_copied: count as u64,
            ..crate::WorkCounters::UNCHARGED
        })
    };
    transferred?;
    copied?;
    Ok(count)
}

pub(super) fn native_builtin_runtime_error(
    error: io::Error,
) -> crate::materializer::NativeTreeMaterializationError {
    crate::materializer::native_materialization_work_error(crate::demand::native_demand_error(
        error,
    ))
}

pub(crate) fn lock_builtin_file(
    file: &File,
    writable: bool,
    work: &crate::materializer::NativeMaterializationWork,
) -> Result<(), crate::materializer::NativeTreeMaterializationError> {
    use crate::materializer::NativeTreeMaterializationError;
    work.charge(READ)?;
    let result = if writable {
        file.try_lock()
    } else {
        file.try_lock_shared()
    };
    match result {
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
pub(super) fn remove_unix_named_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &std::ffi::CStr,
    directory: bool,
    work: &mut W,
) -> Result<(), W::Error> {
    loop {
        let delta = WRITE;
        work.started_io(delta)?;
        // SAFETY: the held directory and terminated single-component name remain live throughout the existing unlink operation.
        if unsafe {
            libc::unlinkat(
                parent.as_raw_fd(),
                name.as_ptr(),
                if directory { libc::AT_REMOVEDIR } else { 0 },
            )
        } == 0
        {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error.into());
        }
    }
}

#[derive(Debug)]
pub(crate) struct NativePreparedAsciiLeaf<const N: usize> {
    #[cfg(unix)]
    pub(super) bytes: [u8; N],
    #[cfg(windows)]
    pub(super) wide: [u16; N],
}

impl<const N: usize> NativePreparedAsciiLeaf<N> {
    pub(crate) fn new(
        bytes: [u8; N],
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, crate::NativeTreeMaterializationError> {
        if N < 2 || bytes[N - 1] != 0 {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout);
        }
        #[cfg(windows)]
        let encoded = (N - 1)
            .checked_mul(std::mem::size_of::<u16>())
            .ok_or(crate::WorkError::Overflow)? as u64;
        let copied = work.reserve_work(WorkCounters {
            items_examined: (N - 1) as u64,
            #[cfg(windows)]
            bytes_encoded: encoded,
            ..WorkCounters::UNCHARGED
        })?;
        let mut copied = copied;
        #[cfg(windows)]
        let mut wide = [0u16; N];
        for index in 0..N - 1 {
            work.check()?;
            let byte = bytes[index];
            let valid = byte.is_ascii() && !matches!(byte, 0 | b'/' | b'\\' | b':');
            #[cfg(windows)]
            {
                wide[index] = u16::from(byte);
            }
            copied.record_completed(WorkCounters {
                items_examined: 1,
                #[cfg(windows)]
                bytes_encoded: std::mem::size_of::<u16>() as u64,
                ..WorkCounters::UNCHARGED
            })?;
            if !valid {
                return Err(crate::NativeTreeMaterializationError::InvalidLayout);
            }
        }
        if &bytes[..N - 1] == b"." || &bytes[..N - 1] == b".." {
            return Err(crate::NativeTreeMaterializationError::InvalidLayout);
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
        let name =
            std::ffi::CStr::from_bytes_with_nul(&name.bytes).map_err(|_| W::invalid_request())?;
        remove_unix_named_leaf(parent, name, false, work)
    }
    #[cfg(windows)]
    {
        remove_windows_leaf(parent, &name.wide[..N - 1], false, None, work)
    }
}

#[cfg(unix)]
pub(super) fn unix_mutation<W: crate::demand::SourceDemandReadWork>(
    work: &mut W,
    mut operation: impl FnMut() -> libc::c_int,
) -> Result<(), DemandError> {
    loop {
        native_write(work)?;
        if operation() == 0 {
            return Ok(());
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error.into());
        }
    }
}
