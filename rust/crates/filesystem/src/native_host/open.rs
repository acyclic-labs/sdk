use super::*;

pub(crate) enum NativeOpenOptions {
    Read,
    #[cfg(any(test, feature = "local", feature = "native-mount"))]
    ReadWrite,
    #[cfg(any(test, feature = "native-mount"))]
    CreateNew,
    #[cfg(any(test, feature = "local"))]
    ReadWriteCreateNew,
    #[cfg(any(test, feature = "local", feature = "native-mount"))]
    ReadWriteCreate,
}

#[cfg(any(test, feature = "local"))]
pub(crate) fn create_directories<W: NativeHostWork>(
    path: &Path,
    work: &mut W,
) -> Result<HostRoot, W::Error> {
    let mut base = Path::new(".");
    #[cfg(unix)]
    let directory = {
        for component in path.components() {
            work.started_io(ITEM)?;
            if matches!(component, std::path::Component::ParentDir) {
                return Err(W::invalid_request());
            }
        }
        base = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or(if path.is_absolute() { path } else { base });
        loop {
            // Ambient parents may be aliases; the requested leaf stays no-follow.
            let opened = unix_name(base.as_os_str(), work, |name, work| {
                let flags = libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC;
                #[cfg(target_os = "linux")]
                let flags = flags | libc::O_NOATIME;
                open_unix(libc::AT_FDCWD, name, flags, work).map(Dir::from_std_file)
            });
            match opened {
                Ok(directory) => break directory,
                Err(error)
                    if matches!(
                        W::io_error_kind(&error),
                        Some(io::ErrorKind::NotFound | io::ErrorKind::InvalidFilename)
                    ) =>
                {
                    base = match base.parent() {
                        Some(parent) if !parent.as_os_str().is_empty() => parent,
                        Some(_) if base != Path::new(".") => Path::new("."),
                        _ => return Err(error),
                    };
                }
                Err(error) => return Err(error),
            }
        }
    };
    #[cfg(windows)]
    let directory = {
        if path.is_absolute() {
            base = path;
            while let Some(parent) = base.parent() {
                work.started_io(ITEM)?;
                base = parent;
            }
        }
        open_root_directory(base, work)?.0
    };
    let relative = if base == Path::new(".") {
        path
    } else {
        path.strip_prefix(base).map_err(|_| W::invalid_request())?
    };
    let relative = relative.strip_prefix(".").unwrap_or(relative);
    #[cfg(unix)]
    let directory = directory_path(&directory, relative, true, work)?;
    #[cfg(windows)]
    let directory = with_wide_path(relative, work, |path, work| {
        directory_path(&directory, path, true, work)
    })?;
    // Creation already owns the validated leaf. Return that capability rather
    // than discarding it and making every caller reopen the ambient path.
    let identity = directory_identity(&directory, work)?;
    Ok(HostRoot {
        directory,
        identity,
    })
}

pub(crate) fn directory_path<W: NativeHostWork>(
    parent: &Dir,
    path: &NativePath,
    create: bool,
    work: &mut W,
) -> Result<Dir, W::Error> {
    #[cfg(unix)]
    let mut current = clone_directory(parent, work)?;
    #[cfg(unix)]
    let components = path.components();
    #[cfg(windows)]
    let mut components = path.split(|unit| *unit == 92).filter(|_| !path.is_empty());
    #[cfg(windows)]
    let mut current = {
        let Some(name) = components.next() else {
            return clone_directory(parent, work);
        };
        work.started_io(ITEM)?;
        open_windows_directory_handle(parent, name, create, work)?
    };
    for component in components {
        work.started_io(ITEM)?;
        #[cfg(unix)]
        let std::path::Component::Normal(name) = component else {
            return Err(W::invalid_request());
        };
        #[cfg(windows)]
        let name = component;
        #[cfg(unix)]
        {
            current = open_unix_directory(&current, name, create, work)?;
        }
        #[cfg(windows)]
        {
            current = open_windows_directory_handle(&current, name, create, work)?;
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
    return with_wide_path(name, work, |name, work| {
        let name = windows_leaf_name(name, work)?;
        open_native_leaf(parent, &name, options, work)
    });
    #[cfg(unix)]
    unix_name(name.as_os_str(), work, |name, work| {
        open_native_leaf(parent, name, options, work)
    })
}

fn open_native_leaf<W: NativeHostWork>(
    parent: &Dir,
    #[cfg(windows)] name: &win::UNICODE_STRING,
    #[cfg(unix)] name: &NativeLeaf,
    options: &NativeOpenOptions,
    work: &mut W,
) -> Result<File, W::Error> {
    #[cfg(windows)]
    {
        let disposition = match options {
            NativeOpenOptions::Read => nt::FILE_OPEN,
            #[cfg(any(test, feature = "local", feature = "native-mount"))]
            NativeOpenOptions::ReadWrite => nt::FILE_OPEN,
            #[cfg(any(test, feature = "local", feature = "native-mount"))]
            NativeOpenOptions::ReadWriteCreate => nt::FILE_OPEN_IF,
            #[cfg(any(test, feature = "local", feature = "native-mount"))]
            _ => nt::FILE_CREATE,
        };
        let access = if matches!(options, NativeOpenOptions::Read) {
            fs::FILE_GENERIC_READ
        } else {
            fs::FILE_GENERIC_READ | fs::FILE_GENERIC_WRITE
        };
        open_windows_named_without_reparse(
            parent,
            name,
            access,
            disposition,
            nt::FILE_NON_DIRECTORY_FILE | nt::FILE_SYNCHRONOUS_IO_NONALERT,
            work,
        )
    }
    #[cfg(unix)]
    {
        let mode = match options {
            NativeOpenOptions::Read => libc::O_RDONLY,
            #[cfg(any(test, feature = "local", feature = "native-mount"))]
            NativeOpenOptions::ReadWrite => libc::O_RDWR,
            #[cfg(any(test, feature = "native-mount"))]
            NativeOpenOptions::CreateNew => libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL,
            #[cfg(any(test, feature = "local"))]
            NativeOpenOptions::ReadWriteCreateNew => libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
            #[cfg(any(test, feature = "local", feature = "native-mount"))]
            NativeOpenOptions::ReadWriteCreate => libc::O_RDWR | libc::O_CREAT,
        };
        let file = open_unix(
            parent.as_raw_fd(),
            name,
            mode | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
            work,
        )?;
        if fstat(file.as_raw_fd(), c"", work)?.st_mode & libc::S_IFMT != libc::S_IFREG {
            return Err(W::invalid_request());
        }
        Ok(file)
    }
}

/// Records one bounded native file transfer on the original work ledger.
pub fn file_transfer(
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

pub(crate) fn remove_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &Path,
    directory: bool,
    work: &mut W,
) -> Result<(), W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    return unix_name(name.as_os_str(), work, |name, work| {
        remove_unix_named_leaf(parent, name, directory, work)
    });
    #[cfg(windows)]
    with_wide_path(name, work, |name, work| {
        remove_windows_leaf(parent, name, directory, None, work)
    })
}

#[cfg(feature = "native-mount")]
pub(crate) fn prepare_leaf<W: NativeHostWork>(
    name: &Path,
    work: &mut W,
) -> Result<(Vec<NativeUnit>, W::Allocation), W::Error> {
    require_leaf(name)?;
    #[cfg(unix)]
    return prepare_unix_name(name.as_os_str(), work);
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt as _;
        prepare_wide_units(name.as_os_str().encode_wide(), work)
    }
}

#[cfg(feature = "native-mount")]
pub(crate) fn remove_prepared_leaf<W: NativeHostWork>(
    parent: &Dir,
    name: &[NativeUnit],
    directory: bool,
    work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    return remove_unix_named_leaf(
        parent,
        std::ffi::CStr::from_bytes_with_nul(name).map_err(|_| W::invalid_request())?,
        directory,
        work,
    );
    #[cfg(windows)]
    remove_windows_leaf(parent, name, directory, None, work)
}

pub(super) fn retry_io<W: NativeHostWork, T>(
    work: &mut W,
    delta: WorkCounters,
    mut operation: impl FnMut() -> io::Result<T>,
) -> Result<T, W::Error> {
    loop {
        match work.attempt_io(delta, &mut operation) {
            Err(error) if W::io_error_kind(&error) == Some(io::ErrorKind::Interrupted) => {}
            result => return result,
        }
    }
}

#[cfg(unix)]
pub(super) fn unix_mutation<W: NativeHostWork>(
    work: &mut W,
    mut operation: impl FnMut() -> libc::c_int,
) -> Result<(), W::Error> {
    retry_io(work, WRITE, || {
        if operation() == 0 {
            Ok(())
        } else {
            Err(io::Error::last_os_error())
        }
    })
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

/// A terminated ASCII component whose validation and native encoding are owned
/// by its constructor. Opening and retirement borrow the same prepared name.
#[cfg(any(test, feature = "native-mount"))]
#[derive(Debug)]
pub(crate) struct PreparedAsciiLeaf<const N: usize> {
    units: [NativeUnit; N],
}

#[cfg(any(test, feature = "native-mount"))]
pub(crate) fn prepare_ascii_leaf<const N: usize>(
    bytes: [u8; N],
    work: &NativeMaterializationWork,
) -> Result<PreparedAsciiLeaf<N>, NativeTreeMaterializationError> {
    let Some((&0, prefix)) = bytes.split_last() else {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    };
    if prefix.is_empty() || matches!(prefix, b"." | b"..") {
        return Err(NativeTreeMaterializationError::InvalidLayout);
    }
    let item = WorkCounters {
        items_examined: 1,
        #[cfg(windows)]
        bytes_encoded: 2,
        ..WorkCounters::UNCHARGED
    };
    let credit = work.reserve_work(WorkCounters {
        items_examined: prefix.len() as u64,
        #[cfg(windows)]
        bytes_encoded: prefix
            .len()
            .checked_mul(2)
            .ok_or(crate::WorkError::Overflow)? as u64,
        ..WorkCounters::UNCHARGED
    })?;
    let mut name = [0; N];
    for (&byte, unit) in prefix.iter().zip(&mut name) {
        crate::NativeMaterializationWork::admit(work, WorkCounters::UNCHARGED)?;
        credit.record_completed(item)?;
        if !byte.is_ascii() || matches!(byte, 0 | b'/' | b'\\' | b':') {
            return Err(NativeTreeMaterializationError::InvalidLayout);
        }
        *unit = NativeUnit::from(byte);
    }
    Ok(PreparedAsciiLeaf { units: name })
}

#[cfg(any(test, feature = "native-mount"))]
pub(crate) fn open_ascii_leaf<W: NativeHostWork, const N: usize>(
    parent: &Dir,
    name: &PreparedAsciiLeaf<N>,
    options: &NativeOpenOptions,
    work: &mut W,
) -> Result<File, W::Error> {
    #[cfg(unix)]
    let name =
        std::ffi::CStr::from_bytes_with_nul(&name.units).map_err(|_| W::invalid_request())?;
    #[cfg(windows)]
    let name = windows_counted_name::<W>(&name.units[..N - 1])?;
    #[cfg(windows)]
    let name = &name;
    open_native_leaf(parent, name, options, work)
}

#[cfg(any(test, feature = "native-mount"))]
pub(crate) fn remove_ascii_leaf<W: NativeHostWork, const N: usize>(
    parent: &Dir,
    name: &PreparedAsciiLeaf<N>,
    work: &mut W,
) -> Result<(), W::Error> {
    #[cfg(unix)]
    return remove_unix_named_leaf(
        parent,
        std::ffi::CStr::from_bytes_with_nul(&name.units).map_err(|_| W::invalid_request())?,
        false,
        work,
    );
    #[cfg(windows)]
    {
        let name = windows_counted_name::<W>(&name.units[..N - 1])?;
        remove_windows_named_leaf(parent, &name, false, None, work)
    }
}

pub(super) fn with_parent_leaf<W: NativeHostWork, T>(
    root: &Dir,
    path: &NativePath,
    work: &mut W,
    operation: impl FnOnce(&Dir, &NativeLeaf, &mut W) -> Result<T, W::Error>,
) -> Result<T, W::Error> {
    #[cfg(unix)]
    let (parent, leaf) = (
        path.parent().ok_or_else(W::invalid_request)?,
        path.file_name().ok_or_else(W::invalid_request)?,
    );
    #[cfg(windows)]
    let (parent, leaf) = wide_parent_leaf(path).map_err(|_| W::invalid_request())?;
    #[cfg(unix)]
    let empty = parent.as_os_str().is_empty();
    #[cfg(windows)]
    let empty = parent.is_empty();
    let held = if empty {
        None
    } else {
        Some(directory_path(root, parent, false, work)?)
    };
    let parent = held.as_ref().unwrap_or(root);
    #[cfg(unix)]
    return unix_name(leaf, work, |leaf, work| operation(parent, leaf, work));
    #[cfg(windows)]
    operation(parent, leaf, work)
}

#[cfg(test)]
mod prepared_leaf_tests {
    use super::*;

    #[test]
    fn prepared_ascii_open_reuses_encoding_with_no_allocation_or_encoding_credit()
    -> Result<(), Box<dyn std::error::Error>> {
        let temporary = tempfile::tempdir()?;
        let root = test_root(temporary.path())?;
        let preparation =
            NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                .map_err(|(error, _)| error)?;
        let name = prepare_ascii_leaf(*b"literal & %.lock\0", &preparation)?;
        let mut work = DemandWork::new(
            WorkBudget {
                items_examined: 0,
                allocation_operations: 0,
                peak_allocation_bytes: 0,
                bytes_encoded: 0,
                ..WorkBudget::UNBOUNDED
            },
            &crate::CancellationToken::new(),
        );
        let file = open_ascii_leaf(
            &root.directory,
            &name,
            &NativeOpenOptions::ReadWriteCreate,
            &mut work,
        )?;
        assert!(file.metadata()?.is_file());
        assert_eq!(work.work.bytes_encoded, 0);
        assert_eq!(work.work.allocation_operations, 0);
        assert_eq!(work.live_allocation_bytes(), 0);
        drop(file);
        // A shared original ledger owns an Arc before the operation starts.
        // Its adapter must likewise add no name allocation or encoding work.
        let before = preparation.receipt();
        let live = preparation.live_allocation_bytes();
        let file = open_ascii_leaf(
            &root.directory,
            &name,
            &NativeOpenOptions::Read,
            &mut &preparation,
        )?;
        let after = preparation.receipt();
        assert_eq!(after.bytes_encoded, before.bytes_encoded);
        assert_eq!(after.allocation_operations, before.allocation_operations);
        assert_eq!(after.peak_allocation_bytes, before.peak_allocation_bytes);
        assert_eq!(preparation.live_allocation_bytes(), live);
        drop(file);
        remove_ascii_leaf(&root.directory, &name, &mut work)?;
        assert_eq!(work.work.items_examined, 0);
        assert_eq!(work.work.bytes_encoded, 0);
        assert_eq!(work.work.allocation_operations, 0);
        assert!(!temporary.path().join("literal & %.lock").exists());
        Ok(())
    }

    #[test]
    fn prepared_ascii_constructor_exhausts_every_byte_and_rejects_non_components()
    -> Result<(), Box<dyn std::error::Error>> {
        let work =
            NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &crate::CancellationToken::new())
                .map_err(|(error, _)| error)?;
        for byte in u8::MIN..=u8::MAX {
            let result = prepare_ascii_leaf([b'a', byte, 0], &work);
            assert_eq!(
                result.is_ok(),
                byte.is_ascii() && !matches!(byte, 0 | b'/' | b'\\' | b':')
            );
            if let Ok(name) = result {
                assert_eq!(
                    name.units,
                    [NativeUnit::from(b'a'), NativeUnit::from(byte), 0]
                );
            }
        }
        assert!(prepare_ascii_leaf([], &work).is_err());
        assert!(prepare_ascii_leaf([0], &work).is_err());
        assert!(prepare_ascii_leaf(*b".\0", &work).is_err());
        assert!(prepare_ascii_leaf(*b"..\0", &work).is_err());
        assert!(prepare_ascii_leaf(*b"unterminated", &work).is_err());
        Ok(())
    }
}
